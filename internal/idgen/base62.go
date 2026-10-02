// Package idgen turns sequential integer IDs into short, non-guessable slugs.
//
// Pipeline: block allocator (unique int) -> Feistel scramble (bijection) -> base62.
package idgen

import (
	"errors"
	"fmt"
	"strings"
)

const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// ErrInvalidSlug is returned by Decode for characters outside the alphabet.
var ErrInvalidSlug = errors.New("idgen: invalid base62 character")

var decodeTable = func() [256]int8 {
	var t [256]int8
	for i := range t {
		t[i] = -1
	}
	for i := 0; i < len(alphabet); i++ {
		t[alphabet[i]] = int8(i)
	}
	return t
}()

// Encode renders n in base62. Encode(0) is "0".
func Encode(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [11]byte // 62^11 > 2^64
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = alphabet[n%62]
		n /= 62
	}
	return string(buf[i:])
}

// Decode is the inverse of Encode. It does not detect uint64 overflow on
// inputs longer than 11 characters; callers should length-check first.
func Decode(s string) (uint64, error) {
	if s == "" {
		return 0, ErrInvalidSlug
	}
	var n uint64
	for i := 0; i < len(s); i++ {
		d := decodeTable[s[i]]
		if d < 0 {
			return 0, fmt.Errorf("%w: %q", ErrInvalidSlug, s[i])
		}
		n = n*62 + uint64(d)
	}
	return n, nil
}

// ValidAlphabet reports whether every byte of s is a base62 character.
func ValidAlphabet(s string) bool {
	return s != "" && strings.IndexFunc(s, func(r rune) bool {
		return r > 255 || decodeTable[r] < 0
	}) < 0
}
