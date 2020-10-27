package genesis

import (
	"encoding/json"
	"math"
	"time"

	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	tmtypes "github.com/tendermint/tendermint/types"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common/identity"
	"github.com/oasisprotocol/oasis-core/go/common/version"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/genesis"
	tendermint "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/crypto"
	genesis "github.com/oasisprotocol/oasis-core/go/genesis/api"
	genesisTestHelpers "github.com/oasisprotocol/oasis-core/go/genesis/tests"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	scheduler "github.com/oasisprotocol/oasis-core/go/scheduler/api"
	stakingTests "github.com/oasisprotocol/oasis-core/go/staking/tests/debug"
)

var _ tendermint.GenesisProvider = (*testNodeGenesisProvider)(nil)

type testNodeGenesisProvider struct {
	document   *genesis.Document
	tmDocument *tmtypes.GenesisDoc
}

func (p *testNodeGenesisProvider) GetGenesisDocument() (*genesis.Document, error) {
	return p.document, nil
}

func (p *testNodeGenesisProvider) GetTendermintGenesisDocument() (*tmtypes.GenesisDoc, error) {
	return p.tmDocument, nil
}

// NewTestNodeGenesisProvider creates a synthetic genesis document for
// running a single node "network", only for testing.
func NewTestNodeGenesisProvider(identity *identity.Identity) (genesis.Provider, error) {
	doc := &genesis.Document{
		Height:    1,
		ChainID:   genesisTestHelpers.TestChainID,
		Time:      time.Now(),
		HaltEpoch: beacon.EpochTime(math.MaxUint64),
		Beacon: beacon.Genesis{
			Parameters: beacon.ConsensusParameters{
				Backend:            beacon.BackendInsecure,
				DebugMockBackend:   true,
				InsecureParameters: &beacon.InsecureParameters{},
			},
		},
		Registry: registry.Genesis{
			Parameters: registry.ConsensusParameters{
				DebugAllowUnroutableAddresses:          true,
				DebugAllowTestRuntimes:                 true,
				DebugAllowEntitySignedNodeRegistration: true,
				DebugBypassStake:                       true,
			},
		},
		Scheduler: scheduler.Genesis{
			Parameters: scheduler.ConsensusParameters{
				MinValidators:          1,
				MaxValidators:          100,
				MaxValidatorsPerEntity: 100,
				DebugBypassStake:       true,
				DebugStaticValidators:  true,
			},
		},
		RootHash: roothash.Genesis{
			Parameters: roothash.ConsensusParameters{
				DebugDoNotSuspendRuntimes: true,
			},
		},
		Consensus: consensus.Genesis{
			Backend: tendermint.BackendName,
			Parameters: consensus.Parameters{
				TimeoutCommit:     1 * time.Millisecond,
				SkipTimeoutCommit: true,
			},
		},
		Staking: stakingTests.DebugGenesisState,
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	tmDoc := &tmtypes.GenesisDoc{
		InitialHeight:   doc.Height,
		ChainID:         doc.ChainID,
		GenesisTime:     doc.Time,
		ConsensusParams: tmtypes.DefaultConsensusParams(),
		AppState:        b,
	}
	tmDoc.ConsensusParams.Version = tmproto.VersionParams{
		AppVersion: version.TendermintAppVersion,
	}

	nodeID := identity.ConsensusSigner.Public()
	pk := crypto.PublicKeyToTendermint(&nodeID)
	validator := tmtypes.GenesisValidator{
		Address: pk.Address(),
		PubKey:  pk,
		Power:   1,
		Name:    "oasis-test-validator-" + nodeID.String(),
	}

	tmDoc.Validators = append(tmDoc.Validators, validator)

	return &testNodeGenesisProvider{
		document:   doc,
		tmDocument: tmDoc,
	}, nil
}
