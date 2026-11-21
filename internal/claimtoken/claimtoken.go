// Package claimtoken signs and verifies claim tokens (agents.md rule 12):
// base64url(payload JSON) "." base64url(Ed25519 signature over the payload
// bytes), both without padding. A token has no expiry; it is valid exactly
// while its claim is current.
package claimtoken

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// ErrInvalid is returned for a token that does not verify.
var ErrInvalid = errors.New("claimtoken: invalid token")

// Payload is the signed claim identity.
type Payload struct {
	TeamID     int64 `json:"team_id"`
	TaskID     int64 `json:"task_id"`
	Attempt    int64 `json:"attempt"`
	ClaimSeq   int64 `json:"claim_seq"`
	ExecutorID int64 `json:"executor_id"`
}

// Signer holds the Ed25519 signing key.
type Signer struct{ key ed25519.PrivateKey }

var enc = base64.RawURLEncoding

// NewSigner parses claim_token.signing_key: a base64 standard Ed25519 seed.
func NewSigner(seed string) (*Signer, error) {
	raw, err := base64.StdEncoding.DecodeString(seed)
	if err != nil || len(raw) != ed25519.SeedSize {
		return nil, errors.New("claimtoken: signing key must be a base64 32-byte Ed25519 seed")
	}
	return &Signer{key: ed25519.NewKeyFromSeed(raw)}, nil
}

// Public returns the verification key.
func (s *Signer) Public() ed25519.PublicKey { return s.key.Public().(ed25519.PublicKey) }

// Sign returns the token of p.
func (s *Signer) Sign(p Payload) (string, error) {
	body, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return enc.EncodeToString(body) + "." + enc.EncodeToString(ed25519.Sign(s.key, body)), nil
}

// Verify checks token with pub and returns its payload.
func Verify(pub ed25519.PublicKey, token string) (Payload, error) {
	part, sigPart, ok := strings.Cut(token, ".")
	if !ok {
		return Payload{}, ErrInvalid
	}
	body, err1 := enc.DecodeString(part)
	sig, err2 := enc.DecodeString(sigPart)
	if err1 != nil || err2 != nil || len(pub) != ed25519.PublicKeySize || !ed25519.Verify(pub, body, sig) {
		return Payload{}, ErrInvalid
	}
	var p Payload
	if err := json.Unmarshal(body, &p); err != nil {
		return Payload{}, ErrInvalid
	}
	return p, nil
}
