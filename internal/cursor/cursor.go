// Package cursor encodes opaque, versioned, HMAC-protected list cursors.
// A cursor binds a scope string (operation, team, operator and every
// filter), the frozen rolling cutoff and the continuation position.
package cursor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/pandops/flowai-task-manager/internal/store"
)

// ErrInvalid is returned for every cursor that is rejected.
var ErrInvalid = errors.New("cursor: invalid")

// MaxLength is the longest accepted cursor.
const MaxLength = 4096

const version = 1

// Key is one HMAC key; the first key of a Codec signs new cursors.
type Key struct {
	ID  int
	Key []byte
}

// Cursor is the decoded content.
type Cursor struct {
	Scope  string
	Cutoff *time.Time
	Pos    store.Position
}

// Codec encodes and decodes cursors.
type Codec struct{ keys []Key }

type content struct {
	V      int        `json:"v"`
	K      int        `json:"k"`
	Scope  string     `json:"s"`
	Cutoff *time.Time `json:"c,omitempty"`
	At     time.Time  `json:"a"`
	Team   int64      `json:"t"`
	Task   int64      `json:"i"`
}

var enc = base64.RawURLEncoding

// New returns a codec; keys must hold at least one 32-byte key.
func New(keys []Key) (*Codec, error) {
	if len(keys) == 0 {
		return nil, errors.New("cursor: at least one key is required")
	}
	seen := map[int]bool{}
	for _, k := range keys {
		if len(k.Key) != 32 || seen[k.ID] {
			return nil, errors.New("cursor: keys must be 32 bytes with distinct ids")
		}
		seen[k.ID] = true
	}
	return &Codec{keys: append([]Key(nil), keys...)}, nil
}

func (k *Codec) key(id int) []byte {
	for _, c := range k.keys {
		if c.ID == id {
			return c.Key
		}
	}
	return nil
}

func mac(key, body []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(body)
	return m.Sum(nil)
}

// Encode returns the opaque token of c.
func (k *Codec) Encode(c Cursor) string {
	signer := k.keys[0]
	body, _ := json.Marshal(content{V: version, K: signer.ID, Scope: c.Scope, Cutoff: c.Cutoff,
		At: c.Pos.IngestedAt.UTC(), Team: c.Pos.TeamID, Task: c.Pos.TaskID})
	return enc.EncodeToString(body) + "." + enc.EncodeToString(mac(signer.Key, body))
}

// Decode verifies and decodes a token.
func (k *Codec) Decode(token string) (Cursor, error) {
	if token == "" || len(token) > MaxLength {
		return Cursor{}, ErrInvalid
	}
	part, sigPart, ok := strings.Cut(token, ".")
	if !ok {
		return Cursor{}, ErrInvalid
	}
	body, err1 := enc.DecodeString(part)
	sig, err2 := enc.DecodeString(sigPart)
	if err1 != nil || err2 != nil {
		return Cursor{}, ErrInvalid
	}
	var c content
	if err := json.Unmarshal(body, &c); err != nil || c.V != version {
		return Cursor{}, ErrInvalid
	}
	key := k.key(c.K)
	if key == nil || !hmac.Equal(sig, mac(key, body)) {
		return Cursor{}, ErrInvalid
	}
	return Cursor{Scope: c.Scope, Cutoff: c.Cutoff, Pos: store.Position{IngestedAt: c.At, TeamID: c.Team, TaskID: c.Task}}, nil
}
