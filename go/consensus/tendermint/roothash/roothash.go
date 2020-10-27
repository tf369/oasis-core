// Package roothash implements the tendermint backed roothash backend.
package roothash

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/eapache/channels"
	"github.com/hashicorp/go-multierror"
	tmabcitypes "github.com/tendermint/tendermint/abci/types"
	tmpubsub "github.com/tendermint/tendermint/libs/pubsub"
	tmrpctypes "github.com/tendermint/tendermint/rpc/core/types"
	tmtypes "github.com/tendermint/tendermint/types"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	tmapi "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	app "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/roothash"
	"github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	runtimeRegistry "github.com/oasisprotocol/oasis-core/go/runtime/registry"
)

const crashPointBlockBeforeIndex = "roothash.before_index"

// ServiceClient is the roothash service client interface.
type ServiceClient interface {
	api.Backend
	tmapi.ServiceClient
}

type runtimeBrokers struct {
	sync.Mutex

	blockNotifier *pubsub.Broker
	eventNotifier *pubsub.Broker

	lastBlockHeight int64
	lastBlock       *block.Block
}

type trackedRuntime struct {
	runtimeID common.Namespace

	height       int64
	blockHistory api.BlockHistory
	reindexDone  bool
}

type cmdTrackRuntime struct {
	runtimeID    common.Namespace
	blockHistory api.BlockHistory
}

type serviceClient struct {
	tmapi.BaseServiceClient
	sync.RWMutex

	ctx    context.Context
	logger *logging.Logger

	backend tmapi.Backend
	querier *app.QueryFactory

	allBlockNotifier *pubsub.Broker
	runtimeNotifiers map[common.Namespace]*runtimeBrokers
	genesisBlocks    map[common.Namespace]*block.Block

	queryCh        chan tmpubsub.Query
	cmdCh          chan interface{}
	trackedRuntime map[common.Namespace]*trackedRuntime
}

func (sc *serviceClient) GetGenesisBlock(ctx context.Context, id common.Namespace, height int64) (*block.Block, error) {
	// First check if we have the genesis blocks cached. They are immutable so easy
	// to cache to avoid repeated requests to the Tendermint app.
	sc.RLock()
	if blk := sc.genesisBlocks[id]; blk != nil {
		sc.RUnlock()
		return blk, nil
	}
	sc.RUnlock()

	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	blk, err := q.GenesisBlock(ctx, id)
	if err != nil {
		return nil, err
	}

	// Update the genesis block cache.
	sc.Lock()
	sc.genesisBlocks[id] = blk
	sc.Unlock()

	return blk, nil
}

func (sc *serviceClient) GetLatestBlock(ctx context.Context, id common.Namespace, height int64) (*block.Block, error) {
	return sc.getLatestBlockAt(ctx, id, height)
}

func (sc *serviceClient) getLatestBlockAt(ctx context.Context, id common.Namespace, height int64) (*block.Block, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	return q.LatestBlock(ctx, id)
}

func (sc *serviceClient) WatchBlocks(id common.Namespace) (<-chan *api.AnnotatedBlock, *pubsub.Subscription, error) {
	notifiers := sc.getRuntimeNotifiers(id)

	sub := notifiers.blockNotifier.SubscribeEx(-1, func(ch channels.Channel) {
		// Replay the latest block if it exists.
		notifiers.Lock()
		defer notifiers.Unlock()
		if notifiers.lastBlock != nil {
			ch.In() <- &api.AnnotatedBlock{
				Height: notifiers.lastBlockHeight,
				Block:  notifiers.lastBlock,
			}
		}
	})
	ch := make(chan *api.AnnotatedBlock)
	sub.Unwrap(ch)

	// Make sure that we only ever emit monotonically increasing blocks. Without
	// special handling this can happen for the first received block due to
	// replaying the latest block (see above).
	invalidRound := uint64(math.MaxUint64)
	lastRound := invalidRound
	monotonicCh := make(chan *api.AnnotatedBlock)
	go func() {
		defer close(monotonicCh)

		for {
			blk, ok := <-ch
			if !ok {
				return
			}
			if lastRound != invalidRound && blk.Block.Header.Round <= lastRound {
				continue
			}
			lastRound = blk.Block.Header.Round
			monotonicCh <- blk
		}
	}()

	// Start tracking this runtime if we are not tracking it yet.
	if err := sc.trackRuntime(sc.ctx, id, nil); err != nil {
		sub.Close()
		return nil, nil, err
	}

	return monotonicCh, sub, nil
}

func (sc *serviceClient) WatchAllBlocks() (<-chan *block.Block, *pubsub.Subscription) {
	sub := sc.allBlockNotifier.Subscribe()
	ch := make(chan *block.Block)
	sub.Unwrap(ch)

	return ch, sub
}

func (sc *serviceClient) WatchEvents(id common.Namespace) (<-chan *api.Event, *pubsub.Subscription, error) {
	notifiers := sc.getRuntimeNotifiers(id)
	sub := notifiers.eventNotifier.Subscribe()
	ch := make(chan *api.Event)
	sub.Unwrap(ch)

	// Start tracking this runtime if we are not tracking it yet.
	if err := sc.trackRuntime(sc.ctx, id, nil); err != nil {
		sub.Close()
		return nil, nil, err
	}

	return ch, sub, nil
}

func (sc *serviceClient) TrackRuntime(ctx context.Context, history api.BlockHistory) error {
	return sc.trackRuntime(ctx, history.RuntimeID(), history)
}

func (sc *serviceClient) trackRuntime(ctx context.Context, id common.Namespace, history api.BlockHistory) error {
	cmd := &cmdTrackRuntime{
		runtimeID:    id,
		blockHistory: history,
	}

	select {
	case sc.cmdCh <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (sc *serviceClient) StateToGenesis(ctx context.Context, height int64) (*api.Genesis, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	return q.Genesis(ctx)
}

func (sc *serviceClient) GetEvents(ctx context.Context, height int64) ([]*api.Event, error) {
	// Get block results at given height.
	var results *tmrpctypes.ResultBlockResults
	results, err := sc.backend.GetBlockResults(ctx, height)
	if err != nil {
		sc.logger.Error("failed to get tendermint block results",
			"err", err,
			"height", height,
		)
		return nil, err
	}

	// Get transactions at given height.
	txns, err := sc.backend.GetTransactions(ctx, height)
	if err != nil {
		sc.logger.Error("failed to get tendermint transactions",
			"err", err,
			"height", height,
		)
		return nil, err
	}

	var events []*api.Event
	// Decode events from block results.
	blockEvs, err := EventsFromTendermint(nil, results.Height, results.BeginBlockEvents)
	if err != nil {
		return nil, err
	}
	events = append(events, blockEvs...)

	blockEvs, err = EventsFromTendermint(nil, results.Height, results.EndBlockEvents)
	if err != nil {
		return nil, err
	}
	events = append(events, blockEvs...)

	// Decode events from transaction results.
	for txIdx, txResult := range results.TxsResults {
		// The order of transactions in txns and results.TxsResults is
		// supposed to match, so the same index in both slices refers to the
		// same transaction.
		evs, txErr := EventsFromTendermint(txns[txIdx], results.Height, txResult.Events)
		if txErr != nil {
			return nil, txErr
		}
		events = append(events, evs...)
	}

	return events, nil
}

func (sc *serviceClient) Cleanup() {
}

func (sc *serviceClient) getRuntimeNotifiers(id common.Namespace) *runtimeBrokers {
	sc.Lock()
	defer sc.Unlock()

	notifiers := sc.runtimeNotifiers[id]
	if notifiers == nil {
		notifiers = &runtimeBrokers{
			blockNotifier: pubsub.NewBroker(false),
			eventNotifier: pubsub.NewBroker(false),
		}
		sc.runtimeNotifiers[id] = notifiers
	}

	return notifiers
}

func (sc *serviceClient) reindexBlocks(currentHeight int64, bh api.BlockHistory) error {
	if currentHeight <= 0 {
		return nil
	}

	logger := sc.logger.With("runtime_id", bh.RuntimeID())

	var err error
	var lastHeight int64
	if lastHeight, err = bh.LastConsensusHeight(); err != nil {
		sc.logger.Error("failed to get last indexed height",
			"err", err,
		)
		return fmt.Errorf("failed to get last indexed height: %w", err)
	}
	// +1 since we want the last non-seen height.
	lastHeight++

	// Take prune strategy into account.
	lastRetainedHeight, err := sc.backend.GetLastRetainedVersion(sc.ctx)
	if err != nil {
		return fmt.Errorf("failed to get last retained height: %w", err)
	}
	if lastHeight < lastRetainedHeight {
		logger.Debug("last height pruned, skipping until last retained",
			"last_retained_height", lastRetainedHeight,
			"last_height", lastHeight,
		)
		lastHeight = lastRetainedHeight
	}

	// Take initial genesis height into account.
	genesisDoc, err := sc.backend.GetGenesisDocument(sc.ctx)
	if err != nil {
		return fmt.Errorf("failed to get genesis document: %w", err)
	}
	if lastHeight < genesisDoc.Height {
		lastHeight = genesisDoc.Height
	}

	// Scan all blocks between last indexed height and current height.
	logger.Debug("reindexing blocks",
		"last_indexed_height", lastHeight,
		"current_height", currentHeight,
		logging.LogEvent, api.LogEventHistoryReindexing,
	)

	for height := lastHeight; height <= currentHeight; height++ {
		var results *tmrpctypes.ResultBlockResults
		results, err = sc.backend.GetBlockResults(sc.ctx, height)
		if err != nil {
			// XXX: could soft-fail first few heights in case more heights were
			// pruned right after the GetLastRetainedVersion query.
			logger.Error("failed to get tendermint block results",
				"err", err,
				"height", height,
			)
			return fmt.Errorf("failed to get tendermint block results: %w", err)
		}

		// Index block.
		tmEvents := append(results.BeginBlockEvents, results.EndBlockEvents...)
		for _, txResults := range results.TxsResults {
			tmEvents = append(tmEvents, txResults.Events...)
		}
		for _, tmEv := range tmEvents {
			if tmEv.GetType() != app.EventType {
				continue
			}

			for _, pair := range tmEv.GetAttributes() {
				if bytes.Equal(pair.GetKey(), app.KeyFinalized) {
					var value app.ValueFinalized
					if err = cbor.Unmarshal(pair.GetValue(), &value); err != nil {
						logger.Error("failed to unmarshal finalized event",
							"err", err,
							"height", height,
						)
						return fmt.Errorf("failed to unmarshal finalized event: %w", err)
					}

					// Only process finalized events for tracked runtimes.
					if sc.trackedRuntime[value.ID] == nil {
						continue
					}
					if err = sc.processFinalizedEvent(sc.ctx, height, value.ID, &value.Round, false); err != nil {
						return fmt.Errorf("failed to process finalized event: %w", err)
					}
				}
			}
		}
	}

	sc.logger.Debug("block reindex complete")

	return nil
}

// Implements api.ServiceClient.
func (sc *serviceClient) ServiceDescriptor() tmapi.ServiceDescriptor {
	return tmapi.NewServiceDescriptor(api.ModuleName, app.EventType, sc.queryCh, sc.cmdCh)
}

// Implements api.ServiceClient.
func (sc *serviceClient) DeliverCommand(ctx context.Context, height int64, cmd interface{}) error {
	switch c := cmd.(type) {
	case *cmdTrackRuntime:
		// Request to track a new runtime.
		etr := sc.trackedRuntime[c.runtimeID]
		if etr != nil {
			// Ignore duplicate runtime tracking requests unless this updates the block history.
			if etr.blockHistory != nil || c.blockHistory == nil {
				break
			}
		} else {
			sc.logger.Debug("tracking new runtime",
				"runtime_id", c.runtimeID,
				"height", height,
			)
		}

		// We need to start watching a new block history.
		tr := &trackedRuntime{
			runtimeID:    c.runtimeID,
			blockHistory: c.blockHistory,
		}
		sc.trackedRuntime[c.runtimeID] = tr
		// Request subscription to events for this runtime.
		sc.queryCh <- app.QueryForRuntime(tr.runtimeID)

		// Emit latest block.
		if err := sc.processFinalizedEvent(ctx, height, tr.runtimeID, nil, true); err != nil {
			sc.logger.Warn("failed to emit latest block",
				"err", err,
				"runtime_id", c.runtimeID,
				"height", height,
			)
		}
		// Make sure we reindex again when receiving the first event.
		tr.reindexDone = false
	default:
		return fmt.Errorf("roothash: unknown command: %T", cmd)
	}
	return nil
}

// Implements api.ServiceClient.
func (sc *serviceClient) DeliverEvent(ctx context.Context, height int64, tx tmtypes.Tx, ev *tmabcitypes.Event) error {
	events, err := EventsFromTendermint(tx, height, []tmabcitypes.Event{*ev})
	if err != nil {
		return fmt.Errorf("roothash: failed to process tendermint events: %w", err)
	}

	for _, ev := range events {
		// Notify non-finalized events.
		if ev.Finalized == nil {
			notifiers := sc.getRuntimeNotifiers(ev.RuntimeID)
			notifiers.eventNotifier.Broadcast(ev)
			continue
		}

		// Only process finalized events for tracked runtimes.
		if sc.trackedRuntime[ev.RuntimeID] == nil {
			continue
		}
		if err = sc.processFinalizedEvent(ctx, height, ev.RuntimeID, &ev.Finalized.Round, true); err != nil {
			return fmt.Errorf("roothash: failed to process finalized event: %w", err)
		}
	}

	return nil
}

func (sc *serviceClient) processFinalizedEvent(
	ctx context.Context,
	height int64,
	runtimeID common.Namespace,
	round *uint64,
	reindex bool,
) (err error) {
	tr := sc.trackedRuntime[runtimeID]
	if tr == nil {
		sc.logger.Error("runtime not tracked",
			"runtime_id", runtimeID,
			"tracked_runtimes", sc.trackedRuntime,
		)
		return fmt.Errorf("roothash: runtime not tracked: %s", runtimeID)
	}
	defer func() {
		// If there was an error, flag the tracked runtime for reindex.
		if err == nil {
			return
		}

		tr.reindexDone = false
	}()

	if height <= tr.height {
		return nil
	}

	// Process finalized event.
	var blk *block.Block
	if blk, err = sc.getLatestBlockAt(ctx, runtimeID, height); err != nil {
		sc.logger.Error("failed to fetch latest block",
			"err", err,
			"height", height,
			"runtime_id", runtimeID,
		)
		return fmt.Errorf("roothash: failed to fetch latest block: %w", err)
	}
	if round != nil && blk.Header.Round != *round {
		sc.logger.Error("finalized event/query round mismatch",
			"block_round", blk.Header.Round,
			"event_round", *round,
		)
		return fmt.Errorf("roothash: finalized event/query round mismatch")
	}

	annBlk := &api.AnnotatedBlock{
		Height: height,
		Block:  blk,
	}

	// Commit the block to history if needed.
	if tr.blockHistory != nil {
		crash.Here(crashPointBlockBeforeIndex)

		// Perform reindex if required.
		if reindex && !tr.reindexDone {
			// Note that we need to reindex up to the previous height as the current height is
			// already being processed right now.
			if err = sc.reindexBlocks(height-1, tr.blockHistory); err != nil {
				sc.logger.Error("failed to reindex blocks",
					"err", err,
					"runtime_id", runtimeID,
				)
				return fmt.Errorf("failed to reindex blocks: %w", err)
			}
			tr.reindexDone = true
		}

		sc.logger.Debug("commit block",
			"runtime_id", runtimeID,
			"height", height,
			"round", blk.Header.Round,
		)

		err = tr.blockHistory.Commit(annBlk)
		if err != nil {
			sc.logger.Error("failed to commit block to history keeper",
				"err", err,
				"runtime_id", runtimeID,
				"height", height,
				"round", blk.Header.Round,
			)
			return fmt.Errorf("failed to commit block to history keeper: %w", err)
		}
	}

	// Skip emitting events if we are reindexing.
	if !reindex {
		return nil
	}

	notifiers := sc.getRuntimeNotifiers(runtimeID)
	// Ensure latest block is set.
	notifiers.Lock()
	notifiers.lastBlock = blk
	notifiers.lastBlockHeight = height
	notifiers.Unlock()

	sc.allBlockNotifier.Broadcast(blk)
	notifiers.blockNotifier.Broadcast(annBlk)
	tr.height = height

	return nil
}

// EventsFromTendermint extracts staking events from tendermint events.
func EventsFromTendermint(
	tx tmtypes.Tx,
	height int64,
	tmEvents []tmabcitypes.Event,
) ([]*api.Event, error) {
	var txHash hash.Hash
	switch tx {
	case nil:
		txHash.Empty()
	default:
		txHash = hash.NewFromBytes(tx)
	}

	var events []*api.Event
	var errs error
	for _, tmEv := range tmEvents {
		// Ignore events that don't relate to the roothash app.
		if tmEv.GetType() != app.EventType {
			continue
		}

		for _, pair := range tmEv.GetAttributes() {
			key := pair.GetKey()
			val := pair.GetValue()

			switch {
			case bytes.Equal(key, app.KeyFinalized):
				// Finalized event.
				var value app.ValueFinalized
				if err := cbor.Unmarshal(val, &value); err != nil {
					errs = multierror.Append(errs, fmt.Errorf("roothash: corrupt Finalized event: %w", err))
					continue
				}

				ev := &api.Event{RuntimeID: value.ID, Height: height, TxHash: txHash, Finalized: &api.FinalizedEvent{Round: value.Round}}
				events = append(events, ev)
			case bytes.Equal(key, app.KeyExecutionDiscrepancyDetected):
				// An execution discrepancy has been detected.
				var value app.ValueExecutionDiscrepancyDetected
				if err := cbor.Unmarshal(val, &value); err != nil {
					errs = multierror.Append(errs, fmt.Errorf("roothash: corrupt ValueExectutionDiscrepancy event: %w", err))
					continue
				}

				ev := &api.Event{RuntimeID: value.ID, Height: height, TxHash: txHash, ExecutionDiscrepancyDetected: &value.Event}
				events = append(events, ev)
			case bytes.Equal(key, app.KeyExecutorCommitted):
				// An executor commit has been processed.
				var value app.ValueExecutorCommitted
				if err := cbor.Unmarshal(val, &value); err != nil {
					errs = multierror.Append(errs, fmt.Errorf("roothash: corrupt ValueExecutorCommitted event: %w", err))
					continue
				}

				ev := &api.Event{RuntimeID: value.ID, Height: height, TxHash: txHash, ExecutorCommitted: &value.Event}
				events = append(events, ev)
			case bytes.Equal(key, app.KeyMessage):
				// Runtime message has been processed.
				var value app.ValueMessage
				if err := cbor.Unmarshal(val, &value); err != nil {
					errs = multierror.Append(errs, fmt.Errorf("roothash: corrupt message event: %w", err))
					continue
				}

				ev := &api.Event{RuntimeID: value.ID, Height: height, TxHash: txHash, Message: &value.Event}
				events = append(events, ev)
			case bytes.Equal(key, app.KeyRuntimeID):
				// Runtime ID attribute (Base64-encoded to allow queries).
			default:
				errs = multierror.Append(errs, fmt.Errorf("roothash: unknown event type: key: %s, val: %s", key, val))
			}
		}
	}
	return events, errs
}

// New constructs a new tendermint-based root hash backend.
func New(
	ctx context.Context,
	dataDir string,
	backend tmapi.Backend,
) (ServiceClient, error) {
	// Initialize and register the tendermint service component.
	a := app.New()
	if err := backend.RegisterApplication(a); err != nil {
		return nil, err
	}

	return &serviceClient{
		ctx:              ctx,
		logger:           logging.GetLogger("roothash/tendermint"),
		backend:          backend,
		querier:          a.QueryFactory().(*app.QueryFactory),
		allBlockNotifier: pubsub.NewBroker(false),
		runtimeNotifiers: make(map[common.Namespace]*runtimeBrokers),
		genesisBlocks:    make(map[common.Namespace]*block.Block),
		queryCh:          make(chan tmpubsub.Query, runtimeRegistry.MaxRuntimeCount),
		cmdCh:            make(chan interface{}, runtimeRegistry.MaxRuntimeCount),
		trackedRuntime:   make(map[common.Namespace]*trackedRuntime),
	}, nil
}

func init() {
	crash.RegisterCrashPoints(
		crashPointBlockBeforeIndex,
	)
}
