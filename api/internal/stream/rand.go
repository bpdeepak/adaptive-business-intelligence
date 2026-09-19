package stream

import (
	"crypto/rand"
	"encoding/hex"
)

// randomHex returns n random bytes hex-encoded (2n characters).
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing is effectively fatal; fall back to zero bytes so
		// the envelope still has a valid (if unlikely) id rather than panicking.
		for i := range b {
			b[i] = byte(i * 7)
		}
	}
	return hex.EncodeToString(b)
}

// newSessionID builds a session id scoped to a replay loop.
func newSessionID(prefix, loopID string) string {
	if prefix == "" {
		prefix = randomHex(6)
	}
	if loopID != "" {
		return prefix + "-" + loopID
	}
	return prefix
}
