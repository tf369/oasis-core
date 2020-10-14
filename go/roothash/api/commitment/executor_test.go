package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
)

func TestConsistentHash(t *testing.T) {
	// NOTE: These hashes MUST be synced with runtime/src/common/roothash.rs.
	var emptyHeaderHash hash.Hash
	_ = emptyHeaderHash.UnmarshalHex("57d73e02609a00fcf4ca43cbf8c9f12867c46942d246fb2b0bce42cbdb8db844")

	var empty ComputeResultsHeader
	require.EqualValues(t, emptyHeaderHash, empty.EncodedHash())

	var emptyRoot hash.Hash
	emptyRoot.Empty()

	var populatedHeaderHash hash.Hash
	_ = populatedHeaderHash.UnmarshalHex("374021bcba44f1014d0d9919e876a1ecd7fe5ec1a92ecf9c8b313cd4976fbc01")

	populated := ComputeResultsHeader{
		Round:        42,
		PreviousHash: emptyHeaderHash,
		IORoot:       &emptyRoot,
		StateRoot:    &emptyRoot,
		Messages:     nil,
	}
	require.EqualValues(t, populatedHeaderHash, populated.EncodedHash())
}

func TestValidateBasic(t *testing.T) {
	var emptyRoot hash.Hash
	emptyRoot.Empty()

	var emptyHeaderHash hash.Hash
	_ = emptyHeaderHash.UnmarshalHex("57d73e02609a00fcf4ca43cbf8c9f12867c46942d246fb2b0bce42cbdb8db844")

	body := ComputeBody{
		Header: ComputeResultsHeader{
			Round:        42,
			PreviousHash: emptyHeaderHash,
			IORoot:       &emptyRoot,
			StateRoot:    &emptyRoot,
			Messages:     nil,
		},
		TxnSchedSig:       signature.Signature{},
		InputRoot:         emptyRoot,
		StorageSignatures: []signature.Signature{{}},
		RakSig:            &signature.RawSignature{},
	}

	for _, tc := range []struct {
		name      string
		fn        func(ComputeBody) ComputeBody
		shouldErr bool
	}{
		{
			"Ok",
			func(b ComputeBody) ComputeBody { return b },
			false,
		},
		{
			"Bad IORoot",
			func(b ComputeBody) ComputeBody {
				b.Header.IORoot = nil
				return b
			},
			true,
		},
		{
			"Bad StateRoot",
			func(b ComputeBody) ComputeBody {
				b.Header.StateRoot = nil
				return b
			},
			true,
		},
		{
			"Bad Failure",
			func(b ComputeBody) ComputeBody {
				b.SetFailure(10)
				return b
			},
			true,
		},
		{
			"Bad Failure",
			func(b ComputeBody) ComputeBody {
				b.Failure = FailureStorageUnavailable
				return b
			},
			true,
		},
		{
			"Ok Failure",
			func(b ComputeBody) ComputeBody {
				b.SetFailure(FailureStorageUnavailable)
				return b
			},
			false,
		},
	} {
		b := tc.fn(body)
		err := b.ValidateBasic()
		switch tc.shouldErr {
		case true:
			require.Error(t, err, "ValidateBasic(%s)", tc.name)
		case false:
			require.NoError(t, err, "ValidateBasic(%s)", tc.name)
		}
	}
}

func TestVerifyStorageReceipt(t *testing.T) {
	rightNs := common.NewTestNamespaceFromSeed([]byte("receipt body verification test"), 0)
	wrongNs := common.NewTestNamespaceFromSeed([]byte("rEcEIpt bOdY vErIfIcAtIOn tEst"), 0)

	var err error

	var emptyRoot hash.Hash
	emptyRoot.Empty()

	var emptyHeaderHash hash.Hash
	_ = emptyHeaderHash.UnmarshalHex("57d73e02609a00fcf4ca43cbf8c9f12867c46942d246fb2b0bce42cbdb8db844")

	body := ComputeBody{
		Header: ComputeResultsHeader{
			Round:        1,
			PreviousHash: emptyHeaderHash,
			IORoot:       &emptyRoot,
			StateRoot:    &emptyRoot,
			Messages:     nil,
		},
		TxnSchedSig:       signature.Signature{},
		InputRoot:         emptyRoot,
		StorageSignatures: []signature.Signature{{}},
		RakSig:            &signature.RawSignature{},
	}

	// Broken storage receipt body.
	receipt := storage.ReceiptBody{
		Version:   1,
		Namespace: rightNs,
		Round:     2,
		RootTypes: []storage.RootType{storage.RootTypeState, storage.RootTypeIO, storage.RootTypeInvalid},
		Roots: []hash.Hash{
			emptyRoot,
			emptyRoot,
			emptyRoot,
		},
	}

	// Go through the various things the function is supposed to check, and
	// slowly fix the receipt in order to get further.

	err = body.VerifyStorageReceipt(wrongNs, &receipt)
	require.EqualError(t, err, "roothash: receipt has unexpected namespace", "wrong namespace")

	err = body.VerifyStorageReceipt(rightNs, &receipt)
	require.EqualError(t, err, "roothash: receipt has unexpected round", "wrong round")
	receipt.Round = 1

	err = body.VerifyStorageReceipt(rightNs, &receipt)
	require.EqualError(t, err, "roothash: receipt has unexpected number of roots", "wrong root count")
	receipt.Roots = receipt.Roots[:2]

	err = body.VerifyStorageReceipt(rightNs, &receipt)
	require.EqualError(t, err, "roothash: receipt has unexpected number of root types", "wrong root type count")
	receipt.RootTypes = receipt.RootTypes[:2]

	err = body.VerifyStorageReceipt(rightNs, &receipt)
	require.EqualError(t, err, "roothash: receipt has unexpected root types", "wrong root type")
	receipt.RootTypes = []storage.RootType{storage.RootTypeIO, storage.RootTypeState}

	err = body.VerifyStorageReceipt(rightNs, &receipt)
	require.NoError(t, err, "correct receipt")
}
