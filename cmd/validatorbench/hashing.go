package main

import (
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klever-io/klever-go/crypto/hashing"
	"github.com/klever-io/klever-go/crypto/hashing/blake2b"
	"github.com/klever-io/klever-go/crypto/hashing/keccak"
	"github.com/klever-io/klever-go/crypto/hashing/sha256"
	"golang.org/x/sys/cpu"
)

// HashStats accumulates the time spent computing hashes and the number of
// hashes produced. It is the primary signal we use to derive
// "hashing time as % of total processing time".
type HashStats struct {
	Bytes     atomic.Uint64
	Hashes    atomic.Uint64
	NanoTotal atomic.Uint64
}

// TimedHasher is a Hasher that records every Compute() call into HashStats.
// It deliberately does NOT replace the underlying hasher used by the VM
// host (which has its own SetHasher pipe via worldhook); instead, callers
// hash transaction-level material (tx hash, merkle, signature digest)
// through this wrapper and contract-level hash work goes through the VM.
//
// The percentage we report is therefore "validator-side" hash time, not
// hash work performed inside the WASM contract — that is what operators
// actually care about when sizing CPU.
type TimedHasher struct {
	inner hashing.Hasher
	stats *HashStats
}

// NewTimedHasher wraps a hashing.Hasher with timing + counters.
func NewTimedHasher(inner hashing.Hasher, stats *HashStats) *TimedHasher {
	return &TimedHasher{inner: inner, stats: stats}
}

// Compute hashes the given input, accumulating timing and byte counters.
// Returns the same bytes as the wrapped hasher.
func (h *TimedHasher) Compute(s string) []byte {
	if h == nil {
		return nil
	}
	start := time.Now()
	out := h.inner.Compute(s)
	h.stats.NanoTotal.Add(uint64(time.Since(start).Nanoseconds()))
	h.stats.Hashes.Add(1)
	h.stats.Bytes.Add(uint64(len(s)))
	return out
}

// EmptyHash forwards to the inner hasher.
func (h *TimedHasher) EmptyHash() []byte { return h.inner.EmptyHash() }

// Size forwards to the inner hasher.
func (h *TimedHasher) Size() int { return h.inner.Size() }

// IsInterfaceNil always returns false; the wrapper itself is never nil.
func (h *TimedHasher) IsInterfaceNil() bool { return h == nil }

// Reset zeroes the counters in-place.
func (s *HashStats) Reset() {
	s.Bytes.Store(0)
	s.Hashes.Store(0)
	s.NanoTotal.Store(0)
}

// HashAccelInfo describes the hardware-acceleration capability for the
// configured hashing algorithm. It is recorded in the report so operators
// can see whether SHA-NI / ARMv8-CE was active during the run.
type HashAccelInfo struct {
	Algorithm    string `json:"algorithm"`
	HWAccelAvail bool   `json:"hw_accel_available"`
	HWAccelUsed  bool   `json:"hw_accel_used"`
	Notes        string `json:"notes"`
}

// DetectHashAccel inspects CPU feature flags to determine whether hardware
// acceleration is available for the chosen algorithm and whether the
// current run is using it (controlled via cfg.SHAHardware + the env var
// set by the re-exec helper).
//
// Notes:
//   - SHA-NI / SHA-512 extensions exist on x86_64 (Intel Goldmont+,
//     AMD Zen, etc.) and arm64 (ARMv8-CE).
//   - blake2b is always pure software in Go (and in OpenSSL); we report
//     "no HW accel" for it.
//   - keccak (sha3) is also software-only on the platforms we target.
func DetectHashAccel(algo string) HashAccelInfo {
	info := HashAccelInfo{Algorithm: algo}
	switch algo {
	case "sha256":
		// Go's stdlib sha256 uses the SHA-NI fast path automatically when
		// available. golang.org/x/sys/cpu does not expose SHA-NI on x86,
		// so we sniff /proc/cpuinfo (linux) or fall back to the ARM flag.
		info.HWAccelAvail = detectSHAExtensions()
		info.HWAccelUsed = info.HWAccelAvail && !hwSHADisabled
		if !info.HWAccelAvail {
			info.Notes = "CPU lacks SHA extensions; software path used"
		} else if !info.HWAccelUsed {
			info.Notes = "HW acceleration disabled via GODEBUG"
		} else {
			info.Notes = "HW acceleration active"
		}
	case "blake2b":
		info.HWAccelAvail = false
		info.Notes = "blake2b has no dedicated HW path on this platform"
	case "keccak":
		info.HWAccelAvail = false
		info.Notes = "keccak/sha3 has no dedicated HW path on this platform"
	}
	return info
}

// NewBaseHasher returns the underlying klever-go hasher implementation
// for the requested algorithm. It is exported so the VM host can be
// constructed with the same hasher the benchmark is timing.
func NewBaseHasher(algo string) (hashing.Hasher, error) {
	switch algo {
	case "sha256":
		return sha256.Sha256{}, nil
	case "keccak":
		return keccak.Keccak{}, nil
	case "blake2b":
		return &blake2b.Blake2b{}, nil
	default:
		return nil, errUnknownHasher
	}
}

// errUnknownHasher mirrors hashing/factory but avoids the import cycle.
var errUnknownHasher = errStr("unknown hash algorithm")

type errStr string

func (e errStr) Error() string { return string(e) }

// hwSHADisabled is set true by the re-exec helper in main when the user
// asked to run without hardware SHA support.
var hwSHADisabled bool

// detectSHAExtensions tries to determine whether the CPU exposes a
// hardware SHA implementation usable by Go's crypto/sha256.
//
// On arm64 this is straightforward: golang.org/x/sys/cpu populates a
// dedicated flag. On x86 the same package does not expose SHA-NI, so
// we read /proc/cpuinfo on linux and look for the "sha_ni" feature.
// On other OSes we conservatively return false.
func detectSHAExtensions() bool {
	if cpu.ARM64.HasSHA2 {
		return true
	}
	if runtime.GOOS == "linux" && (runtime.GOARCH == "amd64" || runtime.GOARCH == "386") {
		data, err := os.ReadFile("/proc/cpuinfo")
		if err != nil {
			return false
		}
		// "flags" line on x86 contains "sha_ni" when SHA-NI is present.
		return strings.Contains(string(data), "sha_ni")
	}
	return false
}

