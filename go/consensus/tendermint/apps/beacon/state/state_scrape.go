package state

import (
	"context"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/keyformat"
	abciAPI "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
)

var (
	// scrapeStateKeyFmt is the current SCRAPE round key format.
	scrapeStateKeyFmt = keyformat.New(0x42)
	// scrapePendingMockEpochKeyFmt is the pending mock epoch key format.
	scrapePendingMockEpochKeyFmt = keyformat.New(0x43)
)

func (s *ImmutableState) SCRAPEState(ctx context.Context) (*beacon.SCRAPEState, error) {
	data, err := s.is.Get(ctx, scrapeStateKeyFmt.Encode())
	if err != nil {
		return nil, abciAPI.UnavailableStateError(err)
	}
	if data == nil {
		return nil, nil
	}

	var state beacon.SCRAPEState
	if err = cbor.Unmarshal(data, &state); err != nil {
		return nil, abciAPI.UnavailableStateError(err)
	}
	return &state, nil
}

func (s *MutableState) SetSCRAPEState(ctx context.Context, state *beacon.SCRAPEState) error {
	err := s.ms.Insert(ctx, scrapeStateKeyFmt.Encode(), cbor.Marshal(state))
	return abciAPI.UnavailableStateError(err)
}

func (s *MutableState) ClearSCRAPEState(ctx context.Context) error {
	err := s.ms.Remove(ctx, scrapeStateKeyFmt.Encode())
	return abciAPI.UnavailableStateError(err)
}

func (s *ImmutableState) SCRAPEPendingMockEpoch(ctx context.Context) (*beacon.EpochTime, error) {
	data, err := s.is.Get(ctx, scrapePendingMockEpochKeyFmt.Encode())
	if err != nil {
		return nil, abciAPI.UnavailableStateError(err)
	}
	if data == nil {
		return nil, nil
	}

	var pendingEpoch beacon.EpochTime
	if err = cbor.Unmarshal(data, &pendingEpoch); err != nil {
		return nil, abciAPI.UnavailableStateError(err)
	}
	return &pendingEpoch, nil
}

func (s *MutableState) SetSCRAPEPendingMockEpoch(ctx context.Context, epoch beacon.EpochTime) error {
	err := s.ms.Insert(ctx, scrapePendingMockEpochKeyFmt.Encode(), cbor.Marshal(epoch))
	return abciAPI.UnavailableStateError(err)
}

func (s *MutableState) ClearSCRAPEPendingMockEpoch(ctx context.Context) error {
	err := s.ms.Remove(ctx, scrapePendingMockEpochKeyFmt.Encode())
	return abciAPI.UnavailableStateError(err)
}
