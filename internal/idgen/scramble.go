package idgen

import "fmt"

const (
	scrambleBits = 40
	halfBits     = scrambleBits / 2
	halfMask     = (1 << halfBits) - 1
	// MaxID is the exclusive upper bound of IDs the scrambler accepts (2^40).
	// 2^40 ~ 1.1e12 links; base62 of that is at most 7 characters.
	MaxID  = uint64(1) << scrambleBits
	rounds = 4
)

// Scrambler is a keyed Feistel permutation over [0, 2^40). It is a bijection,
// so distinct IDs always map to distinct outputs (no collisions), while the
// output looks random so slugs can't be enumerated by incrementing.
//
// This is obfuscation, not encryption: don't rely on it for secrecy.
type Scrambler struct {
	keys [rounds]uint32
}

// NewScrambler derives round keys from a 64-bit secret.
func NewScrambler(secret uint64) *Scrambler {
	s := &Scrambler{}
	x := secret
	for i := range s.keys {
		x = splitmix64(x)
		s.keys[i] = uint32(x)
	}
	return s
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (s *Scrambler) round(r uint32, k uint32) uint32 {
	x := uint64(r)*0x9e3779b1 + uint64(k)
	x = splitmix64(x)
	return uint32(x) & halfMask
}

// Scramble maps id to a unique value in [0, MaxID).
func (s *Scrambler) Scramble(id uint64) (uint64, error) {
	if id >= MaxID {
		return 0, fmt.Errorf("idgen: id %d exceeds 2^%d", id, scrambleBits)
	}
	l, r := uint32(id>>halfBits)&halfMask, uint32(id)&halfMask
	for i := 0; i < rounds; i++ {
		l, r = r, l^s.round(r, s.keys[i])
	}
	return uint64(l)<<halfBits | uint64(r), nil
}

// Unscramble is the inverse of Scramble.
func (s *Scrambler) Unscramble(v uint64) (uint64, error) {
	if v >= MaxID {
		return 0, fmt.Errorf("idgen: value %d exceeds 2^%d", v, scrambleBits)
	}
	l, r := uint32(v>>halfBits)&halfMask, uint32(v)&halfMask
	for i := rounds - 1; i >= 0; i-- {
		l, r = r^s.round(l, s.keys[i]), l
	}
	return uint64(l)<<halfBits | uint64(r), nil
}
