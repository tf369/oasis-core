package beacon

import (
	"context"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	abciAPI "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	beaconState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/beacon/state"
)

// Query is the beacon query interface.
type Query interface {
	Beacon(context.Context) ([]byte, error)
	Epoch(context.Context) (beacon.EpochTime, int64, error)
	Genesis(context.Context) (*beacon.Genesis, error)

	SCRAPEState(context.Context) (*beacon.SCRAPEState, error)
}

// QueryFactory is the beacon query factory.
type QueryFactory struct {
	state abciAPI.ApplicationQueryState
}

// QueryAt returns the beacon query interface for a specific height.
func (sf *QueryFactory) QueryAt(ctx context.Context, height int64) (Query, error) {
	state, err := beaconState.NewImmutableState(ctx, sf.state, height)
	if err != nil {
		return nil, err
	}
	return &beaconQuerier{state}, nil
}

type beaconQuerier struct {
	state *beaconState.ImmutableState
}

func (bq *beaconQuerier) Beacon(ctx context.Context) ([]byte, error) {
	return bq.state.Beacon(ctx)
}

func (bq *beaconQuerier) Epoch(ctx context.Context) (beacon.EpochTime, int64, error) {
	return bq.state.GetEpoch(ctx)
}

func (bq *beaconQuerier) SCRAPEState(ctx context.Context) (*beacon.SCRAPEState, error) {
	return bq.state.SCRAPEState(ctx)
}

func (app *beaconApplication) QueryFactory() interface{} {
	return &QueryFactory{app.state}
}

// NewQueryFactory returns a new QueryFactory backed by the given state
// instance.
func NewQueryFactory(state abciAPI.ApplicationQueryState) *QueryFactory {
	return &QueryFactory{state}
}
