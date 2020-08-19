package api

import (
	"context"
	"fmt"

	"github.com/oasisprotocol/oasis-core/go/common/crypto/scrape"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
)

var (
	// MethodSCRAPECommit is the method name for a SCRAPE commitment.
	MethodSCRAPECommit = transaction.NewMethodName(ModuleName, "SCRAPECommit", SCRAPECommit{})

	// MethodSCRAPEReveal is the method name for a SCRAPE reveal.
	MethodSCRAPEReveal = transaction.NewMethodName(ModuleName, "SCRAPEReveal", SCRAPEReveal{})
)

// SCRAPEParameters are the beacon parameters for the SCRAPE backend.
type SCRAPEParameters struct {
	Participants  uint64 `json:"participants"`
	Threshold     uint64 `json:"threshold"`
	PVSSThreshold uint64 `json:"pvss_threshold"`

	CommitInterval  int64 `json:"commit_interval"`
	RevealInterval  int64 `json:"reveal_interval"`
	TransitionDelay int64 `json:"transition_delay"`

	GasCosts transaction.Costs `json:"gas_costs,omitempty"`
}

const (
	// GasOpSCRAPECommit is the gas operation identifier for a commit.
	GasOpSCRAPECommit transaction.Op = "scrape_commit"
	// GasOpSCRAPEReveal is the gas operation identifier for a reveal.
	GasOpSCRAPEReveal transaction.Op = "scrape_reveal"
)

// DefaultGasCosts are the "default" gas costs for operations.
var DefaultGasCosts = transaction.Costs{
	GasOpSCRAPECommit: 1000,
	GasOpSCRAPEReveal: 1000,
}

// SCRAPECommit is a SCRAPE commitment transaction payload.
type SCRAPECommit struct {
	Epoch EpochTime `json:"epoch"`
	Round uint64    `json:"round"`

	Commit *scrape.Commit `json:"commit,omitempty"`
}

// SCRAPEReveal is a SCRAPE reveal transaction payload.
type SCRAPEReveal struct {
	Epoch EpochTime `json:"epoch"`
	Round uint64    `json:"round"`

	Reveal *scrape.Reveal `json:"reveal,omitempty"`
}

// RoundState is a SCRAPE round state.
type RoundState uint8

const (
	StateInvalid  RoundState = 0
	StateCommit   RoundState = 1
	StateReveal   RoundState = 2
	StateComplete RoundState = 3
)

func (s RoundState) String() string {
	switch s {
	case StateInvalid:
		return "invalid"
	case StateCommit:
		return "commit"
	case StateReveal:
		return "reveal"
	case StateComplete:
		return "complete"
	default:
		return fmt.Sprintf("[invalid state: %d]", s)
	}
}

// SCRAPEState is the SCRAPE backend state.
type SCRAPEState struct {
	Height int64 `json:"height,omitempty"`

	Epoch EpochTime  `json:"epoch,omitempty"`
	Round uint64     `json:"round,omitempty"`
	State RoundState `json:"state,omitempty"`

	Instance     *scrape.Instance      `json:"instance,omitempty"`
	Participants []signature.PublicKey `json:"participants,omitempty"`
	Entropy      []byte                `json:"entropy,omitempty"`

	BadParticipants map[signature.PublicKey]bool `json:"bad_participants,omitempty"`

	CommitDeadline   int64 `json:"commit_deadline,omitempty"`
	RevealDeadline   int64 `json:"reveal_deadline,omitempty"`
	TransitionHeight int64 `json:"transition_height,omitempty"`

	RuntimeDisableHeight int64 `json:"runtime_disable_height,omitempty"`
}

// SCRAPEEvent is a SCRAPE backend event.
type SCRAPEEvent struct {
	Height int64 `json:"height,omitempty"`

	Epoch EpochTime  `json:"epoch,omitempty"`
	Round uint64     `json:"round,omitempty"`
	State RoundState `json:"state,omitempty"`

	Participants []signature.PublicKey `json:"participants,omitempty"`
}

func (ev *SCRAPEEvent) FromState(state *SCRAPEState) {
	ev.Height = state.Height
	ev.Epoch = state.Epoch
	ev.Round = state.Round
	ev.State = state.State
	ev.Participants = state.Participants
}

// SCRAPEBackend is a Backend that is backed by SCRAPE.
type SCRAPEBackend interface {
	Backend

	// GetSCRAPEState gets the SCRAPE beacon round state for the
	// provided block height.  Calling this method with height `0`
	// should return the beacon for the latest finalized block.
	GetSCRAPEState(context.Context, int64) (*SCRAPEState, error)

	// WatchLatestSCRAPEEvent returns a channel that produces a
	// stream of mesages on SCRAPE round events.  If a round
	// transition happens before the previous round event is read
	// from the channel, old events are overwritten.
	//
	// Upon subscription the current round event is sent immediately.
	WatchLatestSCRAPEEvent() (<-chan *SCRAPEEvent, *pubsub.Subscription)
}
