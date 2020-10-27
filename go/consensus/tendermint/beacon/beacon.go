// Package beacon implements the tendermint backed beacon and epochtime
// backends.
package beacon

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/eapache/channels"
	tmabcitypes "github.com/tendermint/tendermint/abci/types"
	tmpubsub "github.com/tendermint/tendermint/libs/pubsub"
	tmtypes "github.com/tendermint/tendermint/types"

	beaconAPI "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	memorySigner "github.com/oasisprotocol/oasis-core/go/common/crypto/signature/signers/memory"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	tmAPI "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	app "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/beacon"
)

var testSigner = memorySigner.NewTestSigner("oasis-core epochtime mock key seed")

// ServiceClient is the beacon service client interface.
type ServiceClient interface {
	beaconAPI.Backend
	tmAPI.ServiceClient
}

type serviceClient struct {
	sync.RWMutex
	tmAPI.BaseServiceClient

	logger  *logging.Logger
	querier *app.QueryFactory

	backend tmAPI.Backend

	epochNotifier     *pubsub.Broker
	epochLastNotified beaconAPI.EpochTime
	epoch             beaconAPI.EpochTime
	epochCurrentBlock int64

	scrapeNotifier     *pubsub.Broker
	scrapeLastNotified hash.Hash
	scrapeEvent        *beaconAPI.SCRAPEEvent

	initialNotify bool

	baseEpoch beaconAPI.EpochTime
	baseBlock int64
}

func (sc *serviceClient) StateToGenesis(ctx context.Context, height int64) (*beaconAPI.Genesis, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	return q.Genesis(ctx)
}

func (sc *serviceClient) GetBaseEpoch(ctx context.Context) (beaconAPI.EpochTime, error) {
	return sc.baseEpoch, nil
}

func (sc *serviceClient) GetEpoch(ctx context.Context, height int64) (beaconAPI.EpochTime, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return beaconAPI.EpochInvalid, err
	}

	epoch, _, err := q.Epoch(ctx)
	return epoch, err
}

func (sc *serviceClient) GetEpochBlock(ctx context.Context, epoch beaconAPI.EpochTime) (int64, error) {
	now, currentBlk := sc.currentEpochBlock()
	if epoch == now {
		return currentBlk, nil
	}

	// The epoch can't be earlier than the initial starting epoch.
	switch {
	case epoch < sc.baseEpoch:
		return -1, fmt.Errorf("epoch predates base (base: %d requested: %d)", sc.baseEpoch, epoch)
	case epoch == sc.baseEpoch:
		return sc.baseBlock, nil
	}

	// Find historic epoch.
	//
	// TODO: This is really really inefficient, and should be optimized,
	// maybe a cache of the last few epochs, or a binary search.
	height := consensus.HeightLatest
	for {
		q, err := sc.querier.QueryAt(ctx, height)
		if err != nil {
			return -1, fmt.Errorf("failed to query epoch: %w", err)
		}

		var pastEpoch beaconAPI.EpochTime
		pastEpoch, height, err = q.Epoch(ctx)
		if err != nil {
			return -1, fmt.Errorf("failed to query epoch: %w", err)
		}

		if epoch == pastEpoch {
			return height, nil
		}

		height--

		// The initial height can be > 1, but presumably this does not
		// matter, since we validate that epoch > sc.baseEpoch.
		if pastEpoch == 0 || height <= 1 {
			return -1, fmt.Errorf("failed to find historic epoch (minimum: %d requested: %d)", pastEpoch, epoch)
		}
	}
}

func (sc *serviceClient) WatchEpochs() (<-chan beaconAPI.EpochTime, *pubsub.Subscription) {
	typedCh := make(chan beaconAPI.EpochTime)
	sub := sc.epochNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (sc *serviceClient) WatchLatestEpoch() (<-chan beaconAPI.EpochTime, *pubsub.Subscription) {
	typedCh := make(chan beaconAPI.EpochTime)
	sub := sc.epochNotifier.SubscribeBuffered(1)
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (sc *serviceClient) GetBeacon(ctx context.Context, height int64) ([]byte, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	return q.Beacon(ctx)
}

func (sc *serviceClient) GetSCRAPEState(ctx context.Context, height int64) (*beaconAPI.SCRAPEState, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	return q.SCRAPEState(ctx)
}

func (sc *serviceClient) WatchLatestSCRAPEEvent() (<-chan *beaconAPI.SCRAPEEvent, *pubsub.Subscription) {
	typedCh := make(chan *beaconAPI.SCRAPEEvent)
	sub := sc.scrapeNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (sc *serviceClient) SetEpoch(ctx context.Context, epoch beaconAPI.EpochTime) error {
	ch, sub := sc.WatchEpochs()
	defer sub.Close()

	tx := transaction.NewTransaction(0, nil, app.MethodSetEpoch, epoch)
	if err := consensus.SignAndSubmitTx(ctx, sc.backend, testSigner, tx); err != nil {
		return fmt.Errorf("epochtime: set epoch failed: %w", err)
	}

	for {
		select {
		case newEpoch, ok := <-ch:
			if !ok {
				return context.Canceled
			}
			if newEpoch == epoch {
				return nil
			}
		case <-ctx.Done():
			return context.Canceled
		}
	}
}

func (sc *serviceClient) ServiceDescriptor() tmAPI.ServiceDescriptor {
	return tmAPI.NewStaticServiceDescriptor("beacon", app.EventType, []tmpubsub.Query{app.QueryApp})
}

func (sc *serviceClient) DeliverBlock(ctx context.Context, height int64) error {
	if sc.initialNotify {
		return nil
	}

	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return fmt.Errorf("epochtime: failed to query state: %w", err)
	}

	var epoch beaconAPI.EpochTime
	epoch, height, err = q.Epoch(ctx)
	if err != nil {
		return fmt.Errorf("epochtime: failed to query epoch: %w", err)
	}

	if sc.updateCachedEpoch(height, epoch) {
		sc.epochNotifier.Broadcast(epoch)
	}

	var scrapeState *beaconAPI.SCRAPEState
	scrapeState, err = q.SCRAPEState(ctx)
	if err != nil {
		return fmt.Errorf("beacon: failed to query SCRAPE state: %w", err)
	}
	if scrapeState != nil {
		var event beaconAPI.SCRAPEEvent
		event.FromState(scrapeState)

		if sc.updateCachedSCRAPEEvent(&event) {
			sc.scrapeNotifier.Broadcast(&event)
		}
	}

	sc.initialNotify = true
	return nil
}

func (sc *serviceClient) DeliverEvent(ctx context.Context, height int64, tx tmtypes.Tx, ev *tmabcitypes.Event) error {
	for _, pair := range ev.GetAttributes() {
		if bytes.Equal(pair.GetKey(), app.KeyEpoch) {
			var epoch beaconAPI.EpochTime
			if err := cbor.Unmarshal(pair.GetValue(), &epoch); err != nil {
				sc.logger.Error("epochtime: malformed epoch",
					"err", err,
				)
				continue
			}

			if sc.updateCachedEpoch(height, epoch) {
				sc.epochNotifier.Broadcast(epoch)
			}
		}
		if bytes.Equal(pair.GetKey(), app.KeySCRAPERound) {
			var event beaconAPI.SCRAPEEvent
			if err := cbor.Unmarshal(pair.GetValue(), &event); err != nil {
				sc.logger.Error("beacon: malformed SCRAPE round",
					"err", err,
				)
				continue
			}
			if sc.updateCachedSCRAPEEvent(&event) {
				sc.scrapeNotifier.Broadcast(&event)
			}
		}
	}
	return nil
}

func (sc *serviceClient) updateCachedEpoch(height int64, epoch beaconAPI.EpochTime) bool {
	sc.Lock()
	defer sc.Unlock()

	sc.epoch = epoch
	sc.epochCurrentBlock = height

	if sc.epochLastNotified != epoch {
		sc.logger.Debug("epoch transition",
			"prev_epoch", sc.epochLastNotified,
			"epoch", epoch,
			"height", height,
		)
		sc.epochLastNotified = sc.epoch
		return true
	}
	return false
}

func (sc *serviceClient) updateCachedSCRAPEEvent(event *beaconAPI.SCRAPEEvent) bool {
	sc.Lock()
	defer sc.Unlock()

	sc.scrapeEvent = event
	cmp := hash.NewFrom(event)

	if !cmp.Equal(&sc.scrapeLastNotified) {
		sc.logger.Debug("scrape round event",
			"epoch", event.Epoch,
			"round", event.Round,
			"state", event.State,
			"height", event.Height,
		)
		sc.scrapeLastNotified = cmp
		return true
	}

	return false
}

func (sc *serviceClient) currentEpochBlock() (beaconAPI.EpochTime, int64) {
	sc.RLock()
	defer sc.RUnlock()

	return sc.epoch, sc.epochCurrentBlock
}

// New constructs a new tendermint backed beacon and epochtime Backend instance.
func New(ctx context.Context, backend tmAPI.Backend) (ServiceClient, error) {
	// Initialize and register the tendermint service component.
	a := app.New()
	if err := backend.RegisterApplication(a); err != nil {
		return nil, err
	}

	sc := &serviceClient{
		logger:  logging.GetLogger("beacon/tendermint"),
		querier: a.QueryFactory().(*app.QueryFactory),
		backend: backend,
	}
	sc.epochNotifier = pubsub.NewBrokerEx(func(ch channels.Channel) {
		sc.RLock()
		defer sc.RUnlock()

		if sc.epochLastNotified == sc.epoch {
			ch.In() <- sc.epoch
		}
	})
	sc.scrapeNotifier = pubsub.NewBrokerEx(func(ch channels.Channel) {
		sc.RLock()
		defer sc.RUnlock()

		if sc.scrapeEvent != nil {
			ch.In() <- sc.scrapeEvent
		}
	})

	genDoc, err := backend.GetGenesisDocument(ctx)
	if err != nil {
		return nil, err
	}

	sc.baseEpoch = genDoc.Beacon.Base
	sc.baseBlock = genDoc.Height

	return sc, nil
}
