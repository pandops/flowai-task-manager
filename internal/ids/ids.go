// Package ids validates FlowAI integer identifiers (design rule 43): a
// positive 64-bit integer, a JSON number in payloads and a decimal token
// without sign or leading zeros in subjects and headers.
package ids

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
)

// ErrInvalid is returned for every value that is not a valid identifier.
var ErrInvalid = errors.New("ids: not a positive 64-bit integer")

// ParseToken parses a decimal token (subject token, header value).
func ParseToken(s string) (int64, error) {
	if s == "" || len(s) > 19 || s[0] == '0' {
		return 0, ErrInvalid
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, ErrInvalid
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 1 {
		return 0, ErrInvalid
	}
	return v, nil
}

// ParseJSON parses a raw JSON value that must be an integer JSON number.
// Strings, fractions, exponents, zero, negatives and values outside int64
// are rejected.
func ParseJSON(raw json.RawMessage) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, ErrInvalid
	}
	return ParseToken(string(raw))
}

// Token formats an identifier as a decimal token.
func Token(v int64) string { return strconv.FormatInt(v, 10) }
