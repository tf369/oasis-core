package beacon

import (
	"bytes"
	"crypto"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"

	"github.com/tendermint/tendermint/abci/types"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/drbg"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/mathrand"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/scrape"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	beaconState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/beacon/state"
	registryState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/registry/state"
	schedulerState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/scheduler/state"
	genesis "github.com/oasisprotocol/oasis-core/go/genesis/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

var (
	// KeySCRAPERound is the ABCI event attribute for specifying a SCRAPE
	// round event.
	KeySCRAPERound = []byte("scrape_round")

	// KeyDisableRuntimes is the ABCI event attribute for signaling
	// that runtimes should be disabled due to beacon failure.
	KeyDisableRuntimes = []byte("disable_runtimes")

	validatorEntropyCtx = []byte("EkB-validator")
)

type backendSCRAPE struct {
	app *beaconApplication
}

func (impl *backendSCRAPE) OnInitChain(
	ctx *api.Context,
	state *beaconState.MutableState,
	params *beacon.ConsensusParameters,
	doc *genesis.Document,
) error {
	// If the backend is configured to use explicitly set epochs, there
	// is nothing further to do.  And yes, this ignores the base epoch,
	// but that's how certain tests are written.
	if params.DebugMockBackend {
		return nil
	}

	// Set the initial epoch.
	baseEpoch := doc.Beacon.Base
	if err := state.SetEpoch(ctx, baseEpoch, ctx.InitialHeight()); err != nil {
		return fmt.Errorf("beacon: failed to set initial epoch: %w", err)
	}

	impl.app.doEmitEpochEvent(ctx, baseEpoch)

	return nil
}

func (impl *backendSCRAPE) OnBeginBlock(
	ctx *api.Context,
	state *beaconState.MutableState,
	params *beacon.ConsensusParameters,
	req types.RequestBeginBlock,
) error {
	scrapeState, err := state.SCRAPEState(ctx)
	if err != nil {
		return fmt.Errorf("beacon: failed to get SCRAPE state: %w", err)
	}

	future, err := state.GetFutureEpoch(ctx)
	if err != nil {
		return fmt.Errorf("beacon: failed to get future epoch: %w", err)
	}
	if future == nil {
		var epoch beacon.EpochTime
		if epoch, _, err = state.GetEpoch(ctx); err != nil {
			return fmt.Errorf("beacon: failed to get current epoch: %w", err)
		}

		if scrapeState == nil {
			// Either this is the initial epoch, or an epoch transition
			// just happened.
			ctx.Logger().Debug("OnBeginBlock: no SCRAPE round pending, rearming")

			return impl.initRound(ctx, state, params, scrapeState, epoch+1)
		}

		return impl.doRoundPeriodic(ctx, state, params, scrapeState, epoch)
	}

	// Round finished and an epoch transition is scheduled.
	if scrapeState.State != beacon.StateComplete {
		return fmt.Errorf("beacon: BUG: invalid state: %d (expected %d)", scrapeState.State, beacon.StateComplete)
	}

	height := ctx.BlockHeight() + 1 // Current height is ctx.BlockHeight() + 1
	switch {
	case future.Height < height:
		// What the fuck, we missed transitioning the epoch?
		ctx.Logger().Error("height mismatch in defered set",
			"height", height,
			"expected_height", future.Height,
		)
		return fmt.Errorf("beacon: height mismatch in defered set")
	case future.Height > height:
		// The epoch transition is scheduled to happen in the grim
		// darkness of the far future.
		return nil
	case future.Height == height:
		// Time to fire the scheduled epoch transition.
	}

	// Transition the epoch.
	ctx.Logger().Info("setting epoch",
		"epoch", future.Epoch,
		"current_height", height,
	)

	if err = state.SetEpoch(ctx, future.Epoch, height); err != nil {
		return fmt.Errorf("beacon: failed to set epoch: %w", err)
	}
	if err = state.ClearFutureEpoch(ctx); err != nil {
		return fmt.Errorf("beacon: failed to clear future epoch: %w", err)
	}
	impl.app.doEmitEpochEvent(ctx, future.Epoch)

	// Derive and broadcast the beacon.
	var b []byte
	switch params.DebugDeterministic {
	case false:
		// In the normal case, use the production context and SCRAPE
		// generated secure entropy.
		b = GetBeacon(future.Epoch, prodEntropyCtx, scrapeState.Entropy)
	case true:
		// UNSAFE/DEBUG - Deterministic beacon.
		//
		// This is for tests only and is rigged such that the we can
		// ensure the deterministically generated node identities get
		// elected to the various committees at the appropriate times.
		//
		// See: go/oasis-test/runner/scenario/e2e/byzantine.go
		b = GetBeacon(future.Epoch, DebugEntropyCtx, DebugEntropy)
	}

	ctx.Logger().Debug("OnBeginBlock: generated beacon",
		"epoch", future.Epoch,
		"beacon", hex.EncodeToString(b),
		"scrape_entropy", hex.EncodeToString(scrapeState.Entropy),
		"height", ctx.BlockHeight(),
	)

	if err = impl.app.onNewBeacon(ctx, b); err != nil {
		return fmt.Errorf("beacon: failed to set beacon: %w", err)
	}

	// Clear out the round state so that the next round is initialized
	// on the next block.  This is done so that the scheduler has an
	// opportunity to pick the next validator set.
	//
	// Note: If runtimes got killed due to prior protocol failures,
	// the upcoming epoch transition will re-enable them.
	if err = state.ClearSCRAPEState(ctx); err != nil {
		return fmt.Errorf("beacon: failed to clear SCRAPE state: %w", err)
	}

	return nil
}

func (impl *backendSCRAPE) doRoundPeriodic( //nolint: gocyclo
	ctx *api.Context,
	state *beaconState.MutableState,
	params *beacon.ConsensusParameters,
	scrapeState *beacon.SCRAPEState,
	epoch beacon.EpochTime,
) error {
	height := ctx.BlockHeight() + 1 // Current height is ctx.BlockHeight() + 1

	// If the height is at the point where the epoch would have
	// transitioned assuming 0 failures, kill runtimes.
	if height == scrapeState.RuntimeDisableHeight && scrapeState.State != beacon.StateComplete {
		ctx.Logger().Warn("OnBeginBlock: runtime disable height reached")

		ctx.EmitEvent(api.NewEventBuilder(impl.app.Name()).Attribute(
			KeyDisableRuntimes,
			nil,
		))
	}

	// To make tests using the mock backend go faster, truncate the commit
	// and reveal periods iff every eligible node has done the right thing.
	if params.DebugMockBackend {
		var (
			delta           int64
			numParticipants = len(scrapeState.Participants)
		)

		switch scrapeState.State {
		case beacon.StateCommit:
			ok, totalCommits := scrapeState.Instance.MayReveal()
			if ok && (totalCommits == numParticipants) {
				ctx.Logger().Debug("OnBeginBlock: accelerating reveal phase transition")

				delta = scrapeState.CommitDeadline - height
				scrapeState.CommitDeadline = height
				scrapeState.RevealDeadline -= delta
			}
		case beacon.StateReveal:
			ok, totalReveals := scrapeState.Instance.MayRecover()
			if ok && (totalReveals == numParticipants) {
				ctx.Logger().Debug("OnBeginBlock: accelerating recovery phase transition")

				delta = scrapeState.RevealDeadline - height
				scrapeState.RevealDeadline = height
			}
		}

		if delta > 0 {
			scrapeState.RuntimeDisableHeight -= delta
			scrapeState.TransitionHeight -= delta
			if err := state.SetSCRAPEState(ctx, scrapeState); err != nil {
				return fmt.Errorf("beacon: failed to set updated SCRAPE state: %w", err)
			}
		}
	}

	// Round in progress.
	switch {
	case height == scrapeState.CommitDeadline:
		ctx.Logger().Debug("OnBeginBlock: height is at commit deadline",
			"height", height,
		)

		if scrapeState.State != beacon.StateCommit {
			return fmt.Errorf("beacon: BUG: invalid state: %d (expected %d)", scrapeState.State, beacon.StateCommit)
		}

		// Persist the nodes that failed to commit.
		var failures []signature.PublicKey
		for idx, id := range scrapeState.Participants {
			if scrapeState.Instance.Commits[idx] == nil {
				failures = append(failures, id)
			}
		}
		impl.appendFailures(scrapeState, failures)

		if ok, totalCommits := scrapeState.Instance.MayReveal(); ok {
			// Update the node status to signify elgibility from the
			// next epoch.
			if err := impl.updateNodeStatus(ctx, state, epoch); err != nil {
				return fmt.Errorf("beacon: failed to update nodes snapshot: %w", err)
			}

			scrapeState.Height = height
			scrapeState.State = beacon.StateReveal
			if err := state.SetSCRAPEState(ctx, scrapeState); err != nil {
				return fmt.Errorf("beacon: failed to set updated SCRAPE state: %w", err)
			}

			impl.doEmitSCRAPEEvent(ctx, scrapeState)
		} else {
			// Round failed: insufficient commits.
			ctx.Logger().Error("round failed, insufficient commits",
				"total_commits", totalCommits,
			)

			return impl.initRound(ctx, state, params, scrapeState, scrapeState.Epoch)
		}
	case height == scrapeState.RevealDeadline:
		ctx.Logger().Debug("OnBeginBlock: height is at reveal deadline",
			"height", height,
		)

		if scrapeState.State != beacon.StateReveal {
			return fmt.Errorf("beacon: BUG: invalid state: %d (expected %d)", scrapeState.State, beacon.StateReveal)
		}

		// Persist the nodes that failed to reveal.
		var failures []signature.PublicKey
		for idx, id := range scrapeState.Participants {
			if scrapeState.Instance.Reveals[idx] == nil {
				failures = append(failures, id)
			}
		}
		impl.appendFailures(scrapeState, failures)

		ok, totalReveals := scrapeState.Instance.MayRecover()
		if ok {
			// Recover the entropy.
			var err error
			scrapeState.Entropy, _, err = scrapeState.Instance.Recover()
			if err != nil {
				return fmt.Errorf("beacon: failed to recover entropy: %w", err)
			}

			scrapeState.Height = height
			scrapeState.State = beacon.StateComplete
			if err = state.SetSCRAPEState(ctx, scrapeState); err != nil {
				return fmt.Errorf("beacon: failed to set updated SCRAPE state: %w", err)
			}

			// XXX: Reward nodes that participated.
			// XXX: Slash nodes and freeze nodes that failed to participate fully.
			impl.doEmitSCRAPEEvent(ctx, scrapeState)

			if params.DebugMockBackend {
				ctx.Logger().Debug("round succeeded with mock backend, doing nothing")
				return nil
			}

			// Schedule the epoch transition.
			return impl.app.scheduleEpochTransitionBlock(
				ctx,
				state,
				scrapeState.Epoch,
				scrapeState.TransitionHeight,
			)
		}

		// Round failed: Insufficient reveals.
		ctx.Logger().Error("round failed, insufficient reveals",
			"total_reveals", totalReveals,
		)

		return impl.initRound(ctx, state, params, scrapeState, scrapeState.Epoch)
	default:
		if scrapeState.State == beacon.StateComplete && params.DebugMockBackend {
			pendingMockEpoch, err := state.SCRAPEPendingMockEpoch(ctx)
			if err != nil {
				return fmt.Errorf("beacon: failed to query mock epoch state: %w", err)
			}
			if pendingMockEpoch == nil {
				// Explicit epoch set tx hasn't happened yet.
				return nil
			}

			if err = state.ClearSCRAPEPendingMockEpoch(ctx); err != nil {
				return fmt.Errorf("beacon: failed to clear mock epoch state: %w", err)
			}

			// Schedule the defered explicit epoch transition.
			return impl.app.scheduleEpochTransitionBlock(
				ctx,
				state,
				*pendingMockEpoch,
				height+1,
			)
		}

		// Still in either the commit or reveal period, nothing to do.
	}

	return nil
}

func (impl *backendSCRAPE) initRound(
	ctx *api.Context,
	state *beaconState.MutableState,
	params *beacon.ConsensusParameters,
	scrapeState *beacon.SCRAPEState,
	epoch beacon.EpochTime,
) error {
	if scrapeState == nil || scrapeState.Epoch != epoch {
		// Yes, this obliterates the bad participant list, since nodes
		// that failed should be frozen now.
		newState := &beacon.SCRAPEState{
			Epoch: epoch,
		}
		scrapeState = newState
	} else {
		// The previous attempt to generate a beacon for this epoch failed.
		scrapeState.Round++
	}

	// Draw participants.
	entropy, err := state.Beacon(ctx)
	if err != nil && err != beacon.ErrBeaconNotAvailable {
		// Beacon not being available is "fine", the pre-sort shuffle
		// is best-effort anyway.
		return fmt.Errorf("beacon: couldn't get shuffle entropy: %w", err)
	}
	schedulerState := schedulerState.NewMutableState(ctx.State())
	registryState := registryState.NewMutableState(ctx.State())
	validators, err := schedulerState.CurrentValidators(ctx)
	if err != nil {
		return fmt.Errorf("beacon: failed to get current validators: %w", err)
	}
	candidateParticipants, err := validatorsByVotingPower(validators, entropy)
	if err != nil {
		return fmt.Errorf("beacon: failed to sort current validators: %w", err)
	}

	numParticipants := int(params.SCRAPEParameters.Participants)
	participants := make([]scrape.Point, 0, numParticipants)
	participantIDs := make([]signature.PublicKey, 0, numParticipants)
	for _, validatorID := range candidateParticipants {
		var node *node.Node
		node, err = registryState.NodeBySubKey(ctx, validatorID)
		if err != nil || node.Beacon == nil {
			continue
		}
		if scrapeState.BadParticipants[node.ID] {
			continue
		}

		participants = append(participants, node.Beacon.Point)
		participantIDs = append(participantIDs, node.ID)
		if len(participants) == numParticipants {
			break
		}
	}
	if l := len(participants); l < numParticipants {
		return fmt.Errorf("beacon: insufficient beacon participants: %d (want %d)", l, numParticipants)
	}

	// Initialize the SCRAPE state.
	if scrapeState.Instance, err = scrape.New(&scrape.Config{
		Participants:  participants,
		Threshold:     int(params.SCRAPEParameters.Threshold),
		PVSSThreshold: int(params.SCRAPEParameters.PVSSThreshold),
	}); err != nil {
		return fmt.Errorf("beacon: failed to initialize SCRAPE instance: %w", err)
	}
	scrapeState.Participants = participantIDs

	// Derive the deadlines.
	//
	// Note: Because of the +1, applied to BlockHeight, it may be required
	// to strategically subtract 1 from one of the three interval/delay
	// parameters (eg: Commit/Reveal/Delay set to 20/10/4 results in
	// transitions at blocsk 35, 70, 105, ...).

	height := ctx.BlockHeight() + 1 // Current height is ctx.BlockHeight() + 1
	scrapeState.CommitDeadline = height + params.SCRAPEParameters.CommitInterval
	scrapeState.RevealDeadline = scrapeState.CommitDeadline + params.SCRAPEParameters.RevealInterval
	scrapeState.TransitionHeight = scrapeState.RevealDeadline + params.SCRAPEParameters.TransitionDelay
	if scrapeState.RuntimeDisableHeight == 0 {
		scrapeState.RuntimeDisableHeight = scrapeState.TransitionHeight
	}

	scrapeState.Height = height
	scrapeState.State = beacon.StateCommit
	if err := state.SetSCRAPEState(ctx, scrapeState); err != nil {
		return fmt.Errorf("beacon: failed to set SCRAPE state: %w", err)
	}

	impl.doEmitSCRAPEEvent(ctx, scrapeState)

	ctx.Logger().Info("initializing SCRAPE round",
		"epoch", epoch,
		"round", scrapeState.Round,
	)

	return nil
}

func (impl *backendSCRAPE) ExecuteTx(
	ctx *api.Context,
	state *beaconState.MutableState,
	params *beacon.ConsensusParameters,
	tx *transaction.Transaction,
) error {
	switch tx.Method {
	case beacon.MethodSCRAPEReveal, beacon.MethodSCRAPECommit:
		// Ensure that the tx is from the current node, to prevent blocks
		// that are loaded with gossiped beacon transactions, that will
		// time out due to the processing overhead.
		//
		// In an ideal world, beacon tx-es shouldn't be gossiped to begin
		// with (and be rejected if received), and each block should be
		// limited to one beacon tx.
		if ctx.IsCheckOnly() && !staking.NewAddress(ctx.TxSigner()).Equal(ctx.AppState().OwnTxSignerAddress()) {
			return fmt.Errorf("beacon: rejecting non-local beacon tx: %s", ctx.TxSigner())
		}
		return impl.doSCRAPETx(ctx, state, params, tx)
	case MethodSetEpoch:
		if !params.DebugMockBackend {
			return fmt.Errorf("beacon: method '%s' is disabled via consensus", MethodSetEpoch)
		}
		return impl.doSetEpochTx(ctx, state, tx)
	default:
		return fmt.Errorf("beacon: invalid method: %s", tx.Method)
	}
}

func (impl *backendSCRAPE) doSCRAPETx(
	ctx *api.Context,
	state *beaconState.MutableState,
	params *beacon.ConsensusParameters,
	tx *transaction.Transaction,
) error {
	scrapeState, err := state.SCRAPEState(ctx)
	if err != nil {
		return fmt.Errorf("beacon: failed to get SCRAPE state: %w", err)
	}
	if scrapeState == nil {
		return fmt.Errorf("beacon: no SCRAPE state, round not in progress")
	}

	// Charge gas for this transaction.
	var gasOp transaction.Op
	switch tx.Method {
	case beacon.MethodSCRAPECommit:
		gasOp = beacon.GasOpSCRAPECommit
	case beacon.MethodSCRAPEReveal:
		gasOp = beacon.GasOpSCRAPEReveal
	}
	if err = ctx.Gas().UseGas(1, gasOp, params.SCRAPEParameters.GasCosts); err != nil {
		return err
	}

	// Ensure the tx is from a current valid participant.
	registryState := registryState.NewMutableState(ctx.State())
	node, err := registryState.Node(ctx, ctx.TxSigner())
	if err != nil {
		return fmt.Errorf("beacon: tx not from a node: %v", err)
	}
	if node.Beacon == nil {
		return fmt.Errorf("beacon: tx signer missing beacon metadata")
	}
	if scrapeState.BadParticipants[ctx.TxSigner()] {
		return fmt.Errorf("beacon: rejecting tx from bad participant")
	}

	participantIdx := -1
	for idx, id := range scrapeState.Participants {
		if id.Equal(node.ID) {
			if !scrapeState.Instance.Participants[idx].Inner().Equal(node.Beacon.Point.Inner()) {
				return fmt.Errorf("beacon: tx signer point updated in registry")
			}
			participantIdx = idx
			break
		}
	}
	if participantIdx < 0 {
		return fmt.Errorf("beacon: tx signer not a participant in the current round")
	}

	// XXX: Slash on failures (should slash-worthy failures BadParticipant a node?).
	switch tx.Method {
	case beacon.MethodSCRAPECommit:
		if err = impl.doCommitTx(ctx, state, scrapeState, tx, participantIdx); err != nil {
			return err
		}
	case beacon.MethodSCRAPEReveal:
		if err = impl.doRevealTx(ctx, state, scrapeState, tx, participantIdx); err != nil {
			return err
		}
	}

	// The transaction was a success, update the state.
	if err = state.SetSCRAPEState(ctx, scrapeState); err != nil {
		return fmt.Errorf("beacon: failed to set updated SCRAPE state: %w", err)
	}

	return nil
}

func (impl *backendSCRAPE) doCommitTx(
	ctx *api.Context,
	state *beaconState.MutableState,
	scrapeState *beacon.SCRAPEState,
	tx *transaction.Transaction,
	participantIdx int,
) error {
	if scrapeState.State != beacon.StateCommit {
		// XXX: Don't slash
		return fmt.Errorf("beacon: unexpected commit tx")
	}

	var commitTx beacon.SCRAPECommit
	if err := cbor.Unmarshal(tx.Body, &commitTx); err != nil {
		return fmt.Errorf("beacon: failed to deserialize commit tx: %w", err)
	}

	// Sanity check the commitment.
	if commitTx.Epoch != scrapeState.Epoch { // XXX: Don't slash?
		return fmt.Errorf("beacon: epoch mismatch in commit tx: %d (expected %d)", commitTx.Epoch, scrapeState.Epoch)
	}
	if commitTx.Round != scrapeState.Round { // XXX: Don't slash?
		return fmt.Errorf("beacon: round mismatch in commit tx: %d (expected %d)", commitTx.Round, scrapeState.Round)
	}
	if commitTx.Commit == nil {
		return fmt.Errorf("beacon: commit tx missing actual commitment")
	}
	if commitTx.Commit.Index != participantIdx {
		return fmt.Errorf("beacon: commit tx index mismatch: %d (expected %d)", commitTx.Commit.Index, participantIdx)
	}

	// Suppress duplicate commits.
	if oldCommit := scrapeState.Instance.Commits[participantIdx]; oldCommit != nil {
		oldHash, newHash := hash.NewFrom(oldCommit), hash.NewFrom(commitTx.Commit)
		if oldHash.Equal(&newHash) {
			// Don't slash, adversaries can replay txes.
			return fmt.Errorf("beacon: commit tx already received for participant: %d", participantIdx)
		}

		return fmt.Errorf("beacon: participant attempted to alter commit: %d", participantIdx)
	}

	// Process the commit (CPU INTENSIVE).
	if err := scrapeState.Instance.OnCommit(commitTx.Commit); err != nil {
		return fmt.Errorf("beacon: failed to proceess commit tx: %w", err)
	}

	return nil
}

func (impl *backendSCRAPE) doRevealTx(
	ctx *api.Context,
	state *beaconState.MutableState,
	scrapeState *beacon.SCRAPEState,
	tx *transaction.Transaction,
	participantIdx int,
) error {
	var revealTx beacon.SCRAPEReveal
	if err := cbor.Unmarshal(tx.Body, &revealTx); err != nil {
		return fmt.Errorf("beacon: failed to deserialize reveal tx: %w", err)
	}

	// Sanity check the reveal.
	if revealTx.Epoch != scrapeState.Epoch { // XXX: Don't slash?
		return fmt.Errorf("beacon: epoch mismatch in reveal tx: %d (expected %d)", revealTx.Epoch, scrapeState.Epoch)
	}
	if revealTx.Round != scrapeState.Round { // XXX: Don't slash?
		return fmt.Errorf("beacon: round mismatch in reveal tx: %d (expected %d)", revealTx.Round, scrapeState.Round)
	}
	if revealTx.Reveal == nil {
		return fmt.Errorf("beacon: reveal tx missing actual reveal")
	}
	if revealTx.Reveal.Index != participantIdx {
		return fmt.Errorf("beacon: reveal tx index mismatch: %d (expected %d)", revealTx.Reveal.Index, participantIdx)
	}

	// Suppress duplicate reveals.
	if oldReveal := scrapeState.Instance.Reveals[participantIdx]; oldReveal != nil {
		oldHash, newHash := hash.NewFrom(oldReveal), hash.NewFrom(revealTx.Reveal)
		if oldHash.Equal(&newHash) {
			// Don't slash, adversaries can replay txes.
			return fmt.Errorf("beacon: reveal tx already received for participant: %d", participantIdx)
		}

		return fmt.Errorf("beacon: participant attempted to alter reveal: %d", participantIdx)
	}

	// Check the state to see if this is permitted.
	switch scrapeState.State {
	case beacon.StateReveal:
	case beacon.StateCommit:
		return fmt.Errorf("beacon: early reveal tx")
	case beacon.StateComplete:
		// XXX: Don't slash
		return fmt.Errorf("beacon: ignoring late reveal tx")
	default:
		// Should never happen.
		return fmt.Errorf("beacon: unexpected reveal tx")
	}

	// Process the commit (CPU INTENSIVE).
	if err := scrapeState.Instance.OnReveal(revealTx.Reveal); err != nil {
		return fmt.Errorf("beacon: failed to process reveal tx: %w", err)
	}

	return nil
}

func (impl *backendSCRAPE) doSetEpochTx(
	ctx *api.Context,
	state *beaconState.MutableState,
	tx *transaction.Transaction,
) error {
	// Handle the mock backend SetEpoch transaction.
	now, _, err := state.GetEpoch(ctx)
	if err != nil {
		return fmt.Errorf("beacon: failed to get current epoch: %w", err)
	}

	var epoch beacon.EpochTime
	if err = cbor.Unmarshal(tx.Body, &epoch); err != nil {
		return fmt.Errorf("beacon: failed to deserialize set epoch tx: %w", err)
	}

	// Ensure there is no SetEpoch call in progress.
	pendingMockEpoch, err := state.SCRAPEPendingMockEpoch(ctx)
	if err != nil {
		return fmt.Errorf("beacon: failed to query mock epoch state: %w", err)
	}
	if pendingMockEpoch != nil {
		// Unless the requested explicit epoch happens to be pending.
		if *pendingMockEpoch == epoch {
			return nil
		}
		return fmt.Errorf("beacon: explicit epoch transition already pending")
	}

	if epoch <= now {
		// Constructing closed timelike curves is left for civilizations
		// that have mastered spacetime metric engineering such as
		// the Xeelee, and has no place in a trivial blockchain project.
		ctx.Logger().Error("explicit epoch transition does not advance time",
			"epoch", now,
			"new_epoch", epoch,
		)
		return fmt.Errorf("beacon: explicit epoch chronology violation")
	}

	if err = state.SetSCRAPEPendingMockEpoch(ctx, epoch); err != nil {
		return fmt.Errorf("beacon: failed to set pending mock epoch: %w", err)
	}

	ctx.Logger().Info("scheduling explicit epoch transition on round completion",
		"epoch", epoch,
	)

	return nil
}

func (impl *backendSCRAPE) doEmitSCRAPEEvent(ctx *api.Context, scrapeState *beacon.SCRAPEState) {
	var event beacon.SCRAPEEvent
	event.FromState(scrapeState)

	ctx.EmitEvent(api.NewEventBuilder(impl.app.Name()).Attribute(
		KeySCRAPERound,
		cbor.Marshal(event),
	))
}

func (impl *backendSCRAPE) appendFailures(scrapeState *beacon.SCRAPEState, failures []signature.PublicKey) {
	if len(failures) == 0 {
		return
	}
	if scrapeState.BadParticipants == nil {
		scrapeState.BadParticipants = make(map[signature.PublicKey]bool)
	}
	for _, id := range failures {
		scrapeState.BadParticipants[id] = true
	}
}

func (impl *backendSCRAPE) updateNodeStatus(ctx *api.Context, state *beaconState.MutableState, epoch beacon.EpochTime) error {
	registryState := registryState.NewMutableState(ctx.State())
	nodes, err := registryState.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("beacon: failed to query node list: %w", err)
	}

	for _, node := range nodes {
		nodeStatus, err := registryState.NodeStatus(ctx, node.ID)
		if err != nil {
			return fmt.Errorf("beacon: failed to query node status: %w", err)
		}
		if nodeStatus.ElectionEligibleAfter != beacon.EpochInvalid {
			// This node is not new, and is already eligible.
			continue
		}

		nodeStatus.ElectionEligibleAfter = epoch
		if err = registryState.SetNodeStatus(ctx, node.ID, nodeStatus); err != nil {
			return fmt.Errorf("beacon: failed to update node status: %w", err)
		}
	}

	return nil
}

func validatorsByVotingPower(m map[signature.PublicKey]int64, entropy []byte) ([]signature.PublicKey, error) {
	// Sort the validators lexographically.
	sorted := make([]signature.PublicKey, 0, len(m))
	for mk := range m {
		sorted = append(sorted, mk)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i][:], sorted[j][:]) < 0
	})

	// To try to make tie-breaks fair, shuffle the validator set first
	// if there is entropy available.
	//
	// Note: This just uses the old beacon since there's a bit of a chicken
	// and egg situation.
	if len(entropy) > 0 {
		drbg, err := drbg.New(crypto.SHA512, entropy, nil, validatorEntropyCtx)
		if err != nil {
			return nil, fmt.Errorf("beacon: couldn't instantiate DRBG: %w", err)
		}
		rngSrc := mathrand.New(drbg)
		rng := rand.New(rngSrc)
		rng.Shuffle(len(sorted), func(i, j int) {
			sorted[i], sorted[j] = sorted[j], sorted[i]
		})
	}

	// Stable-sort the by descending voting power.
	sort.SliceStable(sorted, func(i, j int) bool {
		iPower, jPower := m[sorted[i]], m[sorted[j]]
		return iPower > jPower // Reversed sort.
	})

	return sorted, nil
}
