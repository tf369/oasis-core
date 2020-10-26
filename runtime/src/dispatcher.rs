//! Runtime call dispatcher.
use std::{
    convert::TryInto,
    process,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
};

use anyhow::{anyhow, Result};
use crossbeam::channel;
use io_context::Context;
use slog::Logger;

use crate::{
    common::{
        cbor,
        crypto::{
            hash::Hash,
            signature::{Signature, Signer},
        },
        logger::get_logger,
        roothash::{Block, ComputeResultsHeader, COMPUTE_RESULTS_HEADER_CONTEXT},
    },
    enclave_rpc::{
        demux::Demux as RpcDemux,
        dispatcher::Dispatcher as RpcDispatcher,
        types::{Message as RpcMessage, Request as RpcRequest},
        Context as RpcContext,
    },
    protocol::{Protocol, ProtocolUntrustedLocalStorage},
    rak::RAK,
    storage::{
        mkvs::{
            sync::{HostReadSyncer, NoopReadSyncer},
            Root, RootType, Tree,
        },
        StorageContext,
    },
    transaction::{
        dispatcher::{Dispatcher as TxnDispatcher, NoopDispatcher as TxnNoopDispatcher},
        tree::Tree as TxnTree,
        types::TxnBatch,
        Context as TxnContext,
    },
    types::{Body, ComputedBatch},
};

/// Maximum amount of requests that can be in the dispatcher queue.
const BACKLOG_SIZE: usize = 10;

/// Interface for dispatcher initializers.
pub trait Initializer: Send + Sync {
    /// Initializes the dispatcher(s).
    fn init(
        &self,
        protocol: &Arc<Protocol>,
        rak: &Arc<RAK>,
        rpc_demux: &mut RpcDemux,
        rpc_dispatcher: &mut RpcDispatcher,
    ) -> Option<Box<dyn TxnDispatcher>>;
}

impl<F> Initializer for F
where
    F: Fn(
            &Arc<Protocol>,
            &Arc<RAK>,
            &mut RpcDemux,
            &mut RpcDispatcher,
        ) -> Option<Box<dyn TxnDispatcher>>
        + Send
        + Sync,
{
    fn init(
        &self,
        protocol: &Arc<Protocol>,
        rak: &Arc<RAK>,
        rpc_demux: &mut RpcDemux,
        rpc_dispatcher: &mut RpcDispatcher,
    ) -> Option<Box<dyn TxnDispatcher>> {
        (*self)(protocol, rak, rpc_demux, rpc_dispatcher)
    }
}

type QueueItem = (Context, u64, Body);

/// A guard that will abort the process if dropped while panicking.
///
/// This is to ensure that the runtime will terminate in case there is
/// a panic encountered during dispatch and the runtime is built with
/// a non-abort panic handler.
struct AbortOnPanic;

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if thread::panicking() {
            process::abort();
        }
    }
}

/// Runtime call dispatcher.
pub struct Dispatcher {
    logger: Logger,
    queue_tx: channel::Sender<QueueItem>,
    abort_tx: channel::Sender<()>,
    abort_rx: channel::Receiver<()>,
    protocol: Mutex<Option<Arc<Protocol>>>,
    protocol_cond: Condvar,
    rak: Arc<RAK>,
    abort_batch: Arc<AtomicBool>,
}

impl Dispatcher {
    /// Create a new runtime call dispatcher.
    pub fn new(initializer: Box<dyn Initializer>, rak: Arc<RAK>) -> Arc<Self> {
        let (tx, rx) = channel::bounded(BACKLOG_SIZE);
        let (abort_tx, abort_rx) = channel::bounded(1);

        let dispatcher = Arc::new(Dispatcher {
            logger: get_logger("runtime/dispatcher"),
            queue_tx: tx,
            abort_tx: abort_tx,
            abort_rx: abort_rx,
            protocol: Mutex::new(None),
            protocol_cond: Condvar::new(),
            rak,
            abort_batch: Arc::new(AtomicBool::new(false)),
        });

        let d = dispatcher.clone();
        thread::spawn(move || {
            let _guard = AbortOnPanic;
            d.run(initializer, rx)
        });

        dispatcher
    }

    /// Start the dispatcher.
    pub fn start(&self, protocol: Arc<Protocol>) {
        let mut p = self.protocol.lock().unwrap();
        *p = Some(protocol);
        self.protocol_cond.notify_one();
    }

    /// Queue a new request to be dispatched.
    pub fn queue_request(&self, ctx: Context, id: u64, body: Body) -> Result<()> {
        self.queue_tx.try_send((ctx, id, body))?;
        Ok(())
    }

    /// Signals to dispatcher that it should abort and waits for the abort to
    /// complete.
    pub fn abort_and_wait(&self, ctx: Context, id: u64, req: Body) -> Result<()> {
        self.abort_batch.store(true, Ordering::SeqCst);
        // Queue the request to break the dispatch loop in case nothing is
        // being processed at the moment.
        self.queue_request(ctx, id, req)?;
        // Wait for abort.
        self.abort_rx.recv().map_err(|error| anyhow!("{}", error))
    }

    fn run(
        &self,
        initializer: Box<dyn Initializer>,
        rx: channel::Receiver<QueueItem>,
    ) -> Result<()> {
        // Wait for the protocol instance to be available.
        let protocol = {
            let mut guard = self.protocol.lock().unwrap();
            while guard.is_none() {
                guard = self.protocol_cond.wait(guard).unwrap();
            }

            guard.take().unwrap()
        };

        // Create actual dispatchers for RPCs and transactions.
        info!(self.logger, "Starting the runtime dispatcher");
        let mut rpc_demux = RpcDemux::new(self.rak.clone());
        let mut rpc_dispatcher = RpcDispatcher::new();
        let mut txn_dispatcher: Box<dyn TxnDispatcher> = if let Some(txn) =
            initializer.init(&protocol, &self.rak, &mut rpc_demux, &mut rpc_dispatcher)
        {
            txn
        } else {
            Box::new(TxnNoopDispatcher::new())
        };
        txn_dispatcher.set_abort_batch_flag(self.abort_batch.clone());

        // Create common MKVS to use as a cache as long as the root stays the same. Use separate
        // caches for executing and checking transactions.
        let mut cache = Cache::new(protocol.clone());
        let mut cache_check = Cache::new(protocol.clone());

        'dispatch: loop {
            // Check if abort was requested and if so, signal that the batch
            // was aborted and reset the abort flag.
            if self
                .abort_batch
                .compare_and_swap(true, false, Ordering::SeqCst)
            {
                self.abort_tx.try_send(())?;
            }

            match rx.recv() {
                Ok((ctx, id, Body::RuntimeRPCCallRequest { request })) => {
                    // RPC call.
                    self.dispatch_rpc(
                        &mut rpc_demux,
                        &mut rpc_dispatcher,
                        &protocol,
                        ctx,
                        id,
                        request,
                    );
                }
                Ok((ctx, id, Body::RuntimeLocalRPCCallRequest { request })) => {
                    // Local RPC call.
                    self.dispatch_local_rpc(&mut rpc_dispatcher, &protocol, ctx, id, request);
                }
                Ok((
                    ctx,
                    id,
                    Body::RuntimeExecuteTxBatchRequest {
                        io_root,
                        inputs,
                        block,
                    },
                )) => {
                    // Transaction execution.
                    self.dispatch_txn(
                        &mut cache,
                        &mut txn_dispatcher,
                        &protocol,
                        ctx,
                        id,
                        io_root,
                        inputs,
                        block,
                        false,
                    );
                }
                Ok((ctx, id, Body::RuntimeCheckTxBatchRequest { inputs, block })) => {
                    // Transaction check.
                    self.dispatch_txn(
                        &mut cache_check,
                        &mut txn_dispatcher,
                        &protocol,
                        ctx,
                        id,
                        Hash::default(),
                        inputs,
                        block,
                        true,
                    );
                }
                Ok((ctx, id, Body::RuntimeKeyManagerPolicyUpdateRequest { signed_policy_raw })) => {
                    // KeyManager policy update local RPC call.
                    self.handle_km_policy_update(
                        &mut rpc_dispatcher,
                        &protocol,
                        ctx,
                        id,
                        signed_policy_raw,
                    );
                }
                Ok((_ctx, _id, Body::RuntimeAbortRequest {})) => {
                    // We handle the RuntimeAbortRequest here so that we break
                    // the recv loop and re-check abort flag.
                    info!(self.logger, "Received abort request");
                }
                Ok(_) => {
                    error!(self.logger, "Unsupported request type");
                    break 'dispatch;
                }
                Err(error) => {
                    error!(self.logger, "Error while waiting for request"; "err" => %error);
                    break 'dispatch;
                }
            }
        }

        info!(self.logger, "Runtime call dispatcher is terminating");

        Ok(())
    }

    fn dispatch_txn(
        &self,
        cache: &mut Cache,
        txn_dispatcher: &mut Box<dyn TxnDispatcher>,
        protocol: &Arc<Protocol>,
        ctx: Context,
        id: u64,
        io_root: Hash,
        mut inputs: TxnBatch,
        block: Block,
        check_only: bool,
    ) {
        debug!(self.logger, "Received transaction batch request";
            "state_root" => ?block.header.state_root,
            "round" => block.header.round + 1,
            "check_only" => check_only,
        );

        // Create a new context and dispatch the batch.
        let ctx = ctx.freeze();
        cache.maybe_replace(Root {
            namespace: block.header.namespace,
            version: block.header.round,
            root_type: RootType::State,
            hash: block.header.state_root,
        });

        let untrusted_local = Arc::new(ProtocolUntrustedLocalStorage::new(
            Context::create_child(&ctx),
            protocol.clone(),
        ));
        let txn_ctx = TxnContext::new(ctx.clone(), &block.header, check_only);
        match StorageContext::enter(&mut cache.mkvs, untrusted_local.clone(), || {
            txn_dispatcher.dispatch_batch(&inputs, txn_ctx)
        }) {
            Err(error) => {
                warn!(self.logger, "Dispatching batch error"; "err" => %error);
                protocol
                    .send_response(
                        id,
                        Body::Error {
                            module: "".to_owned(), // XXX: Error codes.
                            code: 0,               // XXX: Error codes.
                            message: format!("{}", error),
                        },
                    )
                    .unwrap();
            }
            Ok((mut outputs, mut tags, messages)) => {
                if check_only {
                    debug!(self.logger, "Transaction batch check complete");

                    // Send the result back.
                    protocol
                        .send_response(id, Body::RuntimeCheckTxBatchResponse { results: outputs })
                        .unwrap();
                } else {
                    // Finalize state.
                    let (state_write_log, new_state_root) = cache
                        .mkvs
                        .commit(
                            Context::create_child(&ctx),
                            block.header.namespace,
                            block.header.round + 1,
                        )
                        .expect("state commit must succeed");
                    txn_dispatcher.finalize(new_state_root);
                    cache.commit(block.header.round + 1, new_state_root);

                    // Generate I/O root. Since we already fetched the inputs we avoid the need
                    // to fetch them again by generating the previous I/O tree (generated by the
                    // transaction scheduler) from the inputs.
                    let mut txn_tree = TxnTree::new(
                        Box::new(NoopReadSyncer),
                        Root {
                            namespace: block.header.namespace,
                            version: block.header.round + 1,
                            root_type: RootType::IO,
                            hash: Hash::empty_hash(),
                        },
                    );
                    let mut hashes = Vec::new();
                    for (batch_order, input) in inputs.drain(..).enumerate() {
                        hashes.push(Hash::digest_bytes(&input));
                        txn_tree
                            .add_input(
                                Context::create_child(&ctx),
                                input,
                                batch_order.try_into().unwrap(),
                            )
                            .expect("add transaction must succeed");
                    }

                    let (_, old_io_root) = txn_tree
                        .commit(Context::create_child(&ctx))
                        .expect("io commit must succeed");
                    if old_io_root != io_root {
                        panic!(
                    "dispatcher: I/O root inconsistent with inputs (expected: {:?} got: {:?})",
                    io_root, old_io_root
                );
                    }

                    for (tx_hash, (output, tags)) in
                        hashes.drain(..).zip(outputs.drain(..).zip(tags.drain(..)))
                    {
                        txn_tree
                            .add_output(Context::create_child(&ctx), tx_hash, output, tags)
                            .expect("add transaction must succeed");
                    }

                    let (io_write_log, io_root) = txn_tree
                        .commit(Context::create_child(&ctx))
                        .expect("io commit must succeed");

                    let header = ComputeResultsHeader {
                        round: block.header.round + 1,
                        previous_hash: block.header.encoded_hash(),
                        io_root: Some(io_root),
                        state_root: Some(new_state_root),
                        messages,
                    };

                    debug!(self.logger, "Transaction batch execution complete";
                        "previous_hash" => ?header.previous_hash,
                        "io_root" => ?header.io_root,
                        "state_root" => ?header.state_root
                    );

                    let rak_sig = if self.rak.public_key().is_some() {
                        self.rak
                            .sign(&COMPUTE_RESULTS_HEADER_CONTEXT, &cbor::to_vec(&header))
                            .unwrap()
                    } else {
                        Signature::default()
                    };

                    let result = ComputedBatch {
                        header,
                        io_write_log,
                        state_write_log,
                        rak_sig,
                    };

                    // Send the result back.
                    protocol
                        .send_response(id, Body::RuntimeExecuteTxBatchResponse { batch: result })
                        .unwrap();
                }
            }
        }
    }

    fn dispatch_rpc(
        &self,
        rpc_demux: &mut RpcDemux,
        rpc_dispatcher: &mut RpcDispatcher,
        protocol: &Arc<Protocol>,
        ctx: Context,
        id: u64,
        request: Vec<u8>,
    ) {
        debug!(self.logger, "Received RPC call request");

        // Process frame.
        let mut buffer = vec![];
        let result = match rpc_demux.process_frame(request, &mut buffer) {
            Ok(result) => result,
            Err(error) => {
                error!(self.logger, "Error while processing frame"; "err" => %error);

                protocol
                    .send_response(
                        id,
                        Body::Error {
                            module: "".to_owned(), // XXX: Error codes.
                            code: 0,               // XXX: Error codes.
                            message: format!("{}", error),
                        },
                    )
                    .unwrap();
                return;
            }
        };

        let protocol_response;
        if let Some((session_id, session_info, message, untrusted_plaintext)) = result {
            // Dispatch request.
            assert!(
                buffer.is_empty(),
                "must have no handshake data in transport mode"
            );

            match message {
                RpcMessage::Request(req) => {
                    // First make sure that the untrusted_plaintext matches
                    // the request's method!
                    if untrusted_plaintext != req.method {
                        error!(self.logger, "Request methods don't match!";
                            "untrusted_plaintext" => ?untrusted_plaintext,
                            "method" => ?req.method
                        );
                        let err_reponse = Body::Error {
                            module: "".to_owned(), // XXX: Error codes.
                            code: 0,               // XXX: Error codes.
                            message: "Request's method doesn't match untrusted_plaintext copy."
                                .to_string(),
                        };
                        protocol.send_response(id, err_reponse).unwrap();
                        return;
                    }

                    // Request, dispatch.
                    let ctx = ctx.freeze();
                    let mut mkvs = Tree::make()
                        .with_root_type(RootType::IO)
                        .new(Box::new(NoopReadSyncer));
                    let untrusted_local = Arc::new(ProtocolUntrustedLocalStorage::new(
                        Context::create_child(&ctx),
                        protocol.clone(),
                    ));
                    let rpc_ctx = RpcContext::new(ctx.clone(), self.rak.clone(), session_info);
                    let response =
                        StorageContext::enter(&mut mkvs, untrusted_local.clone(), || {
                            rpc_dispatcher.dispatch(req, rpc_ctx)
                        });
                    let response = RpcMessage::Response(response);

                    // Note: MKVS commit is omitted, this MUST be global side-effect free.

                    debug!(self.logger, "RPC call dispatch complete");

                    let mut buffer = vec![];
                    match rpc_demux.write_message(session_id, response, &mut buffer) {
                        Ok(_) => {
                            // Transmit response.
                            protocol_response = Body::RuntimeRPCCallResponse { response: buffer };
                        }
                        Err(error) => {
                            error!(self.logger, "Error while writing response"; "err" => %error);
                            protocol_response = Body::Error {
                                module: "".to_owned(), // XXX: Error codes.
                                code: 0,               // XXX: Error codes.
                                message: format!("{}", error),
                            };
                        }
                    }
                }
                RpcMessage::Close => {
                    // Session close.
                    let mut buffer = vec![];
                    match rpc_demux.close(session_id, &mut buffer) {
                        Ok(_) => {
                            // Transmit response.
                            protocol_response = Body::RuntimeRPCCallResponse { response: buffer };
                        }
                        Err(error) => {
                            error!(self.logger, "Error while closing session"; "err" => %error);
                            protocol_response = Body::Error {
                                module: "".to_owned(), // XXX: Error codes.
                                code: 0,               // XXX: Error codes.
                                message: format!("{}", error),
                            };
                        }
                    }
                }
                msg => {
                    warn!(self.logger, "Ignoring invalid RPC message type"; "msg" => ?msg);
                    protocol_response = Body::Error {
                        module: "".to_owned(), // XXX: Error codes.
                        code: 0,               // XXX: Error codes.
                        message: "invalid RPC message type".to_owned(),
                    };
                }
            }
        } else {
            // Send back any handshake frames.
            protocol_response = Body::RuntimeRPCCallResponse { response: buffer };
        }

        protocol.send_response(id, protocol_response).unwrap();
    }

    fn dispatch_local_rpc(
        &self,
        rpc_dispatcher: &mut RpcDispatcher,
        protocol: &Arc<Protocol>,
        ctx: Context,
        id: u64,
        request: Vec<u8>,
    ) {
        debug!(self.logger, "Received local RPC call request");

        let req: RpcRequest = cbor::from_slice(&request).unwrap();

        // Request, dispatch.
        let ctx = ctx.freeze();
        let mut mkvs = Tree::make()
            .with_root_type(RootType::IO)
            .new(Box::new(NoopReadSyncer));
        let untrusted_local = Arc::new(ProtocolUntrustedLocalStorage::new(
            Context::create_child(&ctx),
            protocol.clone(),
        ));
        let rpc_ctx = RpcContext::new(ctx.clone(), self.rak.clone(), None);
        let response = StorageContext::enter(&mut mkvs, untrusted_local.clone(), || {
            rpc_dispatcher.dispatch_local(req, rpc_ctx)
        });
        let response = RpcMessage::Response(response);

        // Note: MKVS commit is omitted, this MUST be global side-effect free.

        debug!(self.logger, "Local RPC call dispatch complete");

        let response = cbor::to_vec(&response);
        let protocol_response = Body::RuntimeLocalRPCCallResponse { response };

        protocol.send_response(id, protocol_response).unwrap();
    }

    fn handle_km_policy_update(
        &self,
        rpc_dispatcher: &mut RpcDispatcher,
        protocol: &Arc<Protocol>,
        _ctx: Context,
        id: u64,
        signed_policy_raw: Vec<u8>,
    ) {
        debug!(self.logger, "Received km policy update request");
        rpc_dispatcher.handle_km_policy_update(signed_policy_raw);
        debug!(self.logger, "KM policy update request complete");

        protocol
            .send_response(id, Body::RuntimeKeyManagerPolicyUpdateResponse {})
            .unwrap();
    }
}

struct Cache {
    protocol: Arc<Protocol>,
    mkvs: Tree,
    root: Root,
}

impl Cache {
    fn new(protocol: Arc<Protocol>) -> Self {
        Self {
            mkvs: Self::new_tree(&protocol, Default::default()),
            root: Default::default(),
            protocol,
        }
    }

    fn new_tree(protocol: &Arc<Protocol>, root: Root) -> Tree {
        let read_syncer = HostReadSyncer::new(protocol.clone());
        Tree::make()
            .with_capacity(100_000, 10_000_000)
            .with_root(root)
            .new(Box::new(read_syncer))
    }

    fn maybe_replace(&mut self, root: Root) {
        if self.root == root {
            return;
        }

        self.mkvs = Self::new_tree(&self.protocol, root);
        self.root = root;
    }

    fn commit(&mut self, version: u64, root_hash: Hash) {
        self.root.version = version;
        self.root.hash = root_hash;
    }
}
