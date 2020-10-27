// Package roothash implements the roothash application.
package roothash

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/tendermint/tendermint/abci/types"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	tmapi "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	beaconapp "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/beacon"
	registryapp "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/registry"
	registryState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/registry/state"
	roothashState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/roothash/state"
	schedulerapp "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/scheduler"
	schedulerState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/scheduler/state"
	stakingapp "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/staking"
	stakingState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/staking/state"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/commitment"
	scheduler "github.com/oasisprotocol/oasis-core/go/scheduler/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

var _ tmapi.Application = (*rootHashApplication)(nil)

type rootHashApplication struct {
	state tmapi.ApplicationState
}

func (app *rootHashApplication) Name() string {
	return AppName
}

func (app *rootHashApplication) ID() uint8 {
	return AppID
}

func (app *rootHashApplication) Methods() []transaction.MethodName {
	return roothash.Methods
}

func (app *rootHashApplication) Blessed() bool {
	return false
}

func (app *rootHashApplication) Dependencies() []string {
	return []string{schedulerapp.AppName, stakingapp.AppName}
}

func (app *rootHashApplication) OnRegister(state tmapi.ApplicationState) {
	app.state = state
}

func (app *rootHashApplication) OnCleanup() {
}

func (app *rootHashApplication) BeginBlock(ctx *tmapi.Context, request types.RequestBeginBlock) error {
	// Check if the beacon has failed in a way that runtimes should be
	// disabled.
	beaconFailed := ctx.HasEvent(beaconapp.AppName, beaconapp.KeyDisableRuntimes)
	// Check if rescheduling has taken place.
	rescheduled := ctx.HasEvent(schedulerapp.AppName, schedulerapp.KeyElected)
	// Check if there was an epoch transition.
	epochChanged, epoch := app.state.EpochChanged(ctx)

	state := roothashState.NewMutableState(ctx.State())

	switch {
	case beaconFailed:
		ctx.Logger().Warn("disabling all transactions, beacon failed")

		if err := state.SetRejectTransactions(ctx); err != nil {
			return fmt.Errorf("failed to set tx disable: %w", err)
		}
	case epochChanged:
		if err := state.ClearRejectTransactions(ctx); err != nil {
			return fmt.Errorf("failed to clear tx disable: %w", err)
		}
		fallthrough
	case rescheduled:
		return app.onCommitteeChanged(ctx, state, epoch)
	}

	return nil
}

func (app *rootHashApplication) onCommitteeChanged(ctx *tmapi.Context, state *roothashState.MutableState, epoch beacon.EpochTime) error {
	schedState := schedulerState.NewMutableState(ctx.State())
	regState := registryState.NewMutableState(ctx.State())
	runtimes, _ := regState.Runtimes(ctx)

	params, err := state.ConsensusParameters(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consensus parameters: %w", err)
	}

	var stakeAcc *stakingState.StakeAccumulatorCache
	if !params.DebugBypassStake {
		stakeAcc, err = stakingState.NewStakeAccumulatorCache(ctx)
		if err != nil {
			return fmt.Errorf("failed to create stake accumulator cache: %w", err)
		}
		defer stakeAcc.Discard()
	}

	for _, rt := range runtimes {
		if !rt.IsCompute() {
			ctx.Logger().Debug("skipping non-compute runtime",
				"runtime", rt.ID,
			)
			continue
		}

		rtState, err := state.RuntimeState(ctx, rt.ID)
		if err != nil {
			return fmt.Errorf("failed to fetch runtime state: %w", err)
		}

		// Since the runtime is in the list of active runtimes in the registry we
		// can safely clear the suspended flag.
		rtState.Suspended = false

		// Prepare new runtime committees based on what the scheduler did.
		executorPool, empty, err := app.prepareNewCommittees(ctx, epoch, rtState, schedState, regState)
		if err != nil {
			return err
		}

		// If there are no committees for this runtime, suspend the runtime as this
		// means that there is noone to pay the maintenance fees.
		//
		// Also suspend the runtime in case the registering entity no longer has enough stake to
		// cover the entity and runtime deposits (this check is skipped if the runtime would be
		// suspended anyway due to nobody being there to pay maintenance fees).
		sufficientStake := true
		if !empty && !params.DebugBypassStake {
			acctAddr := staking.NewAddress(rt.EntityID)
			if err = stakeAcc.CheckStakeClaims(acctAddr); err != nil {
				ctx.Logger().Warn("insufficient stake for runtime operation",
					"err", err,
					"entity", rt.EntityID,
					"account", acctAddr,
				)
				sufficientStake = false
			}
		}
		if (empty || !sufficientStake) && !params.DebugDoNotSuspendRuntimes {
			if err = app.suspendUnpaidRuntime(ctx, rtState, regState); err != nil {
				return err
			}
		}

		// If the committee has actually changed, force a new round.
		if !rtState.Suspended {
			ctx.Logger().Debug("updating committee for runtime",
				"runtime_id", rt.ID,
			)

			// Transition the round.
			ctx.Logger().Debug("new committee, transitioning round",
				"runtime_id", rt.ID,
				"round", rtState.CurrentBlock.Header.Round,
			)

			// Emit an empty epoch transition block in the new round. This is required so that
			// the clients can be sure what state is final when an epoch transition occurs.
			if err = app.emitEmptyBlock(ctx, rtState, block.EpochTransition); err != nil {
				return fmt.Errorf("failed to emit empty block: %w", err)
			}

			// Set the executor pool.
			rtState.ExecutorPool = executorPool
		}

		// Update the runtime descriptor to the latest per-epoch value.
		rtState.Runtime = rt

		if err = state.SetRuntimeState(ctx, rtState); err != nil {
			return fmt.Errorf("failed to set runtime state: %w", err)
		}
	}

	return nil
}

func (app *rootHashApplication) suspendUnpaidRuntime(
	ctx *tmapi.Context,
	rtState *roothashState.RuntimeState,
	regState *registryState.MutableState,
) error {
	ctx.Logger().Warn("maintenance fees not paid for runtime or owner debonded, suspending",
		"runtime_id", rtState.Runtime.ID,
	)

	if err := regState.SuspendRuntime(ctx, rtState.Runtime.ID); err != nil {
		return err
	}

	rtState.Suspended = true
	rtState.ExecutorPool = nil

	// Emity an empty block signalling that the runtime was suspended.
	if err := app.emitEmptyBlock(ctx, rtState, block.Suspended); err != nil {
		return fmt.Errorf("failed to emit empty block: %w", err)
	}

	return nil
}

func (app *rootHashApplication) prepareNewCommittees(
	ctx *tmapi.Context,
	epoch beacon.EpochTime,
	rtState *roothashState.RuntimeState,
	schedState *schedulerState.MutableState,
	regState *registryState.MutableState,
) (
	executorPool *commitment.Pool,
	empty bool,
	err error,
) {
	rtID := rtState.Runtime.ID

	executorPool = new(commitment.Pool)
	executorCommittee, err := schedState.Committee(ctx, scheduler.KindComputeExecutor, rtID)
	if err != nil {
		ctx.Logger().Error("checkCommittees: failed to get executor committee from scheduler",
			"err", err,
			"runtime", rtID,
		)
		return
	}
	if executorCommittee == nil {
		ctx.Logger().Warn("checkCommittees: no executor committee",
			"runtime", rtID,
		)
		empty = true
	} else {
		executorPool = &commitment.Pool{
			Runtime:   rtState.Runtime,
			Committee: executorCommittee,
		}
	}
	return
}

func (app *rootHashApplication) emitEmptyBlock(ctx *tmapi.Context, runtime *roothashState.RuntimeState, hdrType block.HeaderType) error {
	blk := block.NewEmptyBlock(runtime.CurrentBlock, uint64(ctx.Now().Unix()), hdrType)

	runtime.CurrentBlock = blk
	runtime.CurrentBlockHeight = ctx.BlockHeight()
	if runtime.ExecutorPool != nil {
		// Clear timeout if there was one scheduled.
		if runtime.ExecutorPool.NextTimeout != commitment.TimeoutNever {
			state := roothashState.NewMutableState(ctx.State())
			if err := state.ClearRoundTimeout(ctx, runtime.Runtime.ID, runtime.ExecutorPool.NextTimeout); err != nil {
				return fmt.Errorf("failed to clear round timeout: %w", err)
			}
		}
		runtime.ExecutorPool.ResetCommitments()
	}

	tagV := ValueFinalized{
		ID:    runtime.Runtime.ID,
		Round: blk.Header.Round,
	}
	ctx.EmitEvent(
		tmapi.NewEventBuilder(app.Name()).
			Attribute(KeyFinalized, cbor.Marshal(tagV)).
			Attribute(KeyRuntimeID, ValueRuntimeID(runtime.Runtime.ID)),
	)
	return nil
}

func (app *rootHashApplication) ExecuteTx(ctx *tmapi.Context, tx *transaction.Transaction) error {
	state := roothashState.NewMutableState(ctx.State())

	rejectTransactions, err := state.RejectTransactions(ctx)
	if err != nil {
		return err
	}
	if rejectTransactions {
		return fmt.Errorf("roothash: refusing to process transactions, beacon failed")
	}

	switch tx.Method {
	case roothash.MethodExecutorCommit:
		var xc roothash.ExecutorCommit
		if err = cbor.Unmarshal(tx.Body, &xc); err != nil {
			return err
		}

		return app.executorCommit(ctx, state, &xc)
	case roothash.MethodExecutorProposerTimeout:
		var xc roothash.ExecutorProposerTimeoutRequest
		if err = cbor.Unmarshal(tx.Body, &xc); err != nil {
			return err
		}

		return app.executorProposerTimeout(ctx, state, &xc)
	default:
		return roothash.ErrInvalidArgument
	}
}

func (app *rootHashApplication) ForeignExecuteTx(ctx *tmapi.Context, other tmapi.Application, tx *transaction.Transaction) error {
	switch other.Name() {
	case registryapp.AppName:
		for _, ev := range ctx.GetEvents() {
			if ev.Type != registryapp.EventType {
				continue
			}

			for _, pair := range ev.Attributes {
				if bytes.Equal(pair.GetKey(), registryapp.KeyRuntimeRegistered) {
					var rt registry.Runtime
					if err := cbor.Unmarshal(pair.GetValue(), &rt); err != nil {
						return fmt.Errorf("roothash: failed to deserialize new runtime: %w", err)
					}

					ctx.Logger().Debug("ForeignDeliverTx: new runtime",
						"runtime", rt.ID,
					)

					if err := app.onNewRuntime(ctx, &rt, nil); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (app *rootHashApplication) onNewRuntime(ctx *tmapi.Context, runtime *registry.Runtime, genesis *roothash.Genesis) error {
	if !runtime.IsCompute() {
		ctx.Logger().Warn("onNewRuntime: ignoring non-compute runtime",
			"runtime", runtime,
		)
		return nil
	}

	// Check if state already exists for the given runtime.
	state := roothashState.NewMutableState(ctx.State())
	_, err := state.RuntimeState(ctx, runtime.ID)
	switch err {
	case nil:
		ctx.Logger().Warn("onNewRuntime: state for runtime already exists",
			"runtime", runtime,
		)
		return nil
	case roothash.ErrInvalidRuntime:
		// Runtime does not yet exist.
	default:
		return fmt.Errorf("failed to fetch runtime state: %w", err)
	}

	// Create genesis block.
	now := ctx.Now().Unix()
	genesisBlock := block.NewGenesisBlock(runtime.ID, uint64(now))
	// Fill the Header fields with Genesis runtime states, if this was called during InitChain().
	genesisBlock.Header.Round = runtime.Genesis.Round
	genesisBlock.Header.StateRoot = runtime.Genesis.StateRoot
	genesisBlock.Header.StorageSignatures = runtime.Genesis.StorageReceipts
	if ctx.IsInitChain() {
		// NOTE: Outside InitChain the genesis argument will be nil.
		genesisRts := genesis.RuntimeStates[runtime.ID]
		if genesisRts != nil {
			genesisBlock.Header.Round = genesisRts.Round
			genesisBlock.Header.StateRoot = genesisRts.StateRoot
			genesisBlock.Header.StorageSignatures = runtime.Genesis.StorageReceipts
		}
	}

	// Create new state containing the genesis block.
	err = state.SetRuntimeState(ctx, &roothashState.RuntimeState{
		Runtime:      runtime,
		CurrentBlock: genesisBlock,
		GenesisBlock: genesisBlock,
	})
	if err != nil {
		return fmt.Errorf("failed to set runtime state: %w", err)
	}

	ctx.Logger().Debug("onNewRuntime: created genesis state for runtime",
		"runtime", runtime,
	)

	// This transaction now also includes a new block for the given runtime.
	tagV := ValueFinalized{
		ID:    runtime.ID,
		Round: genesisBlock.Header.Round,
	}
	ctx.EmitEvent(
		tmapi.NewEventBuilder(app.Name()).
			Attribute(KeyFinalized, cbor.Marshal(tagV)).
			Attribute(KeyRuntimeID, ValueRuntimeID(runtime.ID)),
	)
	return nil
}

func (app *rootHashApplication) EndBlock(ctx *tmapi.Context, request types.RequestEndBlock) (types.ResponseEndBlock, error) {
	state := roothashState.NewMutableState(ctx.State())

	// Check if any runtimes require round timeouts to expire.
	roundTimeouts, err := state.RuntimesWithRoundTimeouts(ctx, ctx.BlockHeight())
	if err != nil {
		return types.ResponseEndBlock{}, fmt.Errorf("failed to fetch runtimes with round timeouts: %w", err)
	}
	for _, runtimeID := range roundTimeouts {
		if err = app.processRoundTimeout(ctx, state, runtimeID); err != nil {
			return types.ResponseEndBlock{}, fmt.Errorf("failed to process round timeout: %w", err)
		}
	}

	return types.ResponseEndBlock{}, nil
}

func (app *rootHashApplication) processRoundTimeout(ctx *tmapi.Context, state *roothashState.MutableState, runtimeID common.Namespace) error {
	ctx.Logger().Warn("round timeout expired, forcing finalization",
		logging.LogEvent, roothash.LogEventTimerFired,
	)

	rtState, err := state.RuntimeState(ctx, runtimeID)
	if err != nil {
		return fmt.Errorf("failed to get runtime state: %w", err)
	}

	if !rtState.ExecutorPool.IsTimeout(ctx.BlockHeight()) {
		// This should NEVER happen.
		ctx.Logger().Error("no scheduled timeout",
			"runtime_id", runtimeID,
			"height", ctx.BlockHeight(),
			"next_timeout", rtState.ExecutorPool.NextTimeout,
		)
		return fmt.Errorf("no scheduled timeout")
	}

	if err = app.tryFinalizeBlock(ctx, rtState, true); err != nil {
		ctx.Logger().Error("failed to finalize block",
			"err", err,
		)
		return fmt.Errorf("failed to finalize block: %w", err)
	}

	if err = state.SetRuntimeState(ctx, rtState); err != nil {
		return fmt.Errorf("failed to set runtime state: %w", err)
	}
	return nil
}

// tryFinalizeExecutorCommits tries to finalize the executor commitments into a new runtime block.
// The caller must take care of clearing and scheduling the round timeouts.
func (app *rootHashApplication) tryFinalizeExecutorCommits(
	ctx *tmapi.Context,
	rtState *roothashState.RuntimeState,
	forced bool,
) (*block.Block, error) {
	runtime := rtState.Runtime
	blockNr := rtState.CurrentBlock.Header.Round

	commit, err := rtState.ExecutorPool.TryFinalize(ctx.BlockHeight(), runtime.Executor.RoundTimeout, forced, true)
	switch err {
	case nil:
		// Round has been finalized.
		ctx.Logger().Debug("finalized round",
			"round", blockNr,
		)

		// Generate the final block.
		hdr := commit.ToDDResult().(commitment.ComputeResultsHeader)

		blk := block.NewEmptyBlock(rtState.CurrentBlock, uint64(ctx.Now().Unix()), block.Normal)
		blk.Header.IORoot = *hdr.IORoot
		blk.Header.StateRoot = *hdr.StateRoot
		// Messages omitted on purpose.

		// Timeout will be cleared by caller.
		rtState.ExecutorPool.ResetCommitments()

		return blk, nil
	case commitment.ErrStillWaiting:
		// Need more commits.
		ctx.Logger().Debug("insufficient commitments for finality, waiting",
			"round", blockNr,
		)

		return nil, nil
	case commitment.ErrDiscrepancyDetected:
		// Discrepancy has been detected.
		ctx.Logger().Warn("executor discrepancy detected",
			"round", blockNr,
			logging.LogEvent, roothash.LogEventExecutionDiscrepancyDetected,
		)

		tagV := ValueExecutionDiscrepancyDetected{
			ID: runtime.ID,
			Event: roothash.ExecutionDiscrepancyDetectedEvent{
				Timeout: forced,
			},
		}
		ctx.EmitEvent(
			tmapi.NewEventBuilder(app.Name()).
				Attribute(KeyExecutionDiscrepancyDetected, cbor.Marshal(tagV)).
				Attribute(KeyRuntimeID, ValueRuntimeID(runtime.ID)),
		)
		return nil, nil
	default:
	}

	// Something else went wrong, emit empty error block.
	ctx.Logger().Error("round failed",
		"round", blockNr,
		"err", err,
		logging.LogEvent, roothash.LogEventRoundFailed,
	)

	if err := app.emitEmptyBlock(ctx, rtState, block.RoundFailed); err != nil {
		return nil, fmt.Errorf("failed to emit empty block: %w", err)
	}

	return nil, nil
}

func (app *rootHashApplication) postProcessFinalizedBlock(ctx *tmapi.Context, rtState *roothashState.RuntimeState, blk *block.Block) error {
	sc := ctx.StartCheckpoint()
	defer sc.Close()

	for _, message := range blk.Header.Messages {
		// Currently there are no valid roothash messages, so any message
		// is treated as unsatisfactory. This is the place which would
		// otherwise contain message handlers.
		unsat := errors.New("tendermint/roothash: message is invalid")

		if unsat != nil {
			ctx.Logger().Error("handler not satisfied with message",
				"err", unsat,
				"message", message,
				logging.LogEvent, roothash.LogEventMessageUnsat,
			)

			// Substitute empty block.
			if err := app.emitEmptyBlock(ctx, rtState, block.RoundFailed); err != nil {
				return fmt.Errorf("failed to emit empty block: %w", err)
			}

			return nil
		}
	}

	sc.Commit()

	// All good. Hook up the new block.
	rtState.CurrentBlock = blk
	rtState.CurrentBlockHeight = ctx.BlockHeight()

	tagV := ValueFinalized{
		ID:    rtState.Runtime.ID,
		Round: blk.Header.Round,
	}
	ctx.EmitEvent(
		tmapi.NewEventBuilder(app.Name()).
			Attribute(KeyFinalized, cbor.Marshal(tagV)).
			Attribute(KeyRuntimeID, ValueRuntimeID(rtState.Runtime.ID)),
	)
	return nil
}

func (app *rootHashApplication) tryFinalizeBlock(
	ctx *tmapi.Context,
	rtState *roothashState.RuntimeState,
	forced bool,
) (err error) {
	defer func(previousTimeout int64) {
		if err != nil {
			return
		}

		// Do not re-arm the round timeout if the timeout has not changed.
		nextTimeout := rtState.ExecutorPool.NextTimeout
		if previousTimeout == nextTimeout {
			return
		}

		state := roothashState.NewMutableState(ctx.State())
		if previousTimeout != commitment.TimeoutNever {
			if err = state.ClearRoundTimeout(ctx, rtState.Runtime.ID, previousTimeout); err != nil {
				err = fmt.Errorf("failed to clear round timeout: %w", err)
				return
			}
		}

		switch nextTimeout {
		case commitment.TimeoutNever:
			// Only clear round timeout (already done).
			ctx.Logger().Debug("disarming round timeout")
		default:
			// Set a different round timeout.
			ctx.Logger().Debug("(re-)arming round timeout",
				"height", ctx.BlockHeight(),
				"next_timeout", nextTimeout,
			)
			if err = state.ScheduleRoundTimeout(ctx, rtState.Runtime.ID, nextTimeout); err != nil {
				err = fmt.Errorf("failed to schedule round timeout: %w", err)
				return
			}
		}
	}(rtState.ExecutorPool.NextTimeout)

	finalizedBlock, err := app.tryFinalizeExecutorCommits(ctx, rtState, forced)
	if err != nil {
		return fmt.Errorf("failed to finalize executor commits: %w", err)
	}
	if finalizedBlock == nil {
		return nil
	}

	if err = app.postProcessFinalizedBlock(ctx, rtState, finalizedBlock); err != nil {
		return fmt.Errorf("failed to post process finalized block: %w", err)
	}
	return nil
}

// New constructs a new roothash application instance.
func New() tmapi.Application {
	return &rootHashApplication{}
}
