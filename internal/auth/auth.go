// Package auth provides opt-in API key authentication for the gRPC API.
package auth

import (
	"crypto/subtle"
	"errors"
)

var (
	ErrMissingKey = errors.New("auth: missing API key")
	ErrInvalidKey = errors.New("auth: invalid API key")
)

// CheckKey validates presented keys against the expected key using a
// constant-time comparison. An empty expected key disables authentication.
func CheckKey(expected string, presented []string) error {
	if expected == "" {
		return nil
	}
	if len(presented) == 0 {
		return ErrMissingKey
	}
	for _, p := range presented {
		if subtle.ConstantTimeCompare([]byte(p), []byte(expected)) == 1 {
			return nil
		}
	}
	return ErrInvalidKey
}
