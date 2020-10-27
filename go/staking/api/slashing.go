package api

import (
	"fmt"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common/quantity"
)

// SlashReason is the reason why a node was slashed.
type SlashReason int

const (
	// SlashDoubleSigning is slashing due to double signing.
	SlashDoubleSigning SlashReason = 0

	// SlashDoubleSigningName is the string representation of SlashDoubleSigning.
	SlashDoubleSigningName = "double-signing"
)

// String returns a string representation of a SlashReason.
func (s SlashReason) String() string {
	switch s {
	case SlashDoubleSigning:
		return SlashDoubleSigningName
	default:
		return "[unknown slash reason]"
	}
}

// MarshalText encodes a SlashReason into text form.
func (s SlashReason) MarshalText() ([]byte, error) {
	switch s {
	case SlashDoubleSigning:
		return []byte(SlashDoubleSigningName), nil
	default:
		return nil, fmt.Errorf("invalid slash reason: %d", s)
	}
}

// UnmarshalText decodes a text slice into a SlashReason.
func (s *SlashReason) UnmarshalText(text []byte) error {
	switch string(text) {
	case SlashDoubleSigningName:
		*s = SlashDoubleSigning
	default:
		return fmt.Errorf("invalid slash reason: %s", string(text))
	}
	return nil
}

// Slash is the per-reason slashing configuration.
type Slash struct {
	Amount         quantity.Quantity `json:"amount"`
	FreezeInterval beacon.EpochTime  `json:"freeze_interval"`
}
