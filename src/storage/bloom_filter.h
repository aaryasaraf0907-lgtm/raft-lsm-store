// Bloom filter for SSTables, exactly as specified in docs/sstable-format.md §7.
//
// Hash:   BloomHash(key) = Fmix64(Fnv1a64(key bytes))                    (§7.1)
// Probes: h1 = h & 0xFFFFFFFF, h2 = (h >> 32) | 1,
//         bit_i = (h1 + i * h2) mod num_bits  for i in [0, num_probes)   (§7.2)
// Bits:   bit b lives in byte b >> 3 at position b & 7, LSB first        (§7.3)
// Format: [hash_kind u8 = 1][num_probes u8][reserved u16 = 0][num_bits u64][bits]
//         little-endian, total 12 + num_bits / 8 bytes                    (§7.5)
//
// MayContain() == false means the key is definitely absent; true means "possibly present".
// A filter never reports an inserted key as absent.
//
// Errors (team decision M3): invalid build parameters (bits_per_key outside [1, 64]) and
// filter-size overflow return kInvalidArgument; malformed serialized data passed to Parse
// returns kCorruption.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "storage/kv_store.h"

namespace raftkv {

class BloomFilter {
 public:
  static constexpr uint8_t kHashKind = 1;
  static constexpr uint32_t kDefaultBitsPerKey = 10;
  static constexpr uint32_t kMinBitsPerKey = 1;
  static constexpr uint32_t kMaxBitsPerKey = 64;
  static constexpr uint32_t kMaxProbes = 30;
  static constexpr uint64_t kMinBits = 64;
  static constexpr size_t kHeaderBytes = 12;

  // Empty in-memory filter: kMinBits zero bits, probes for kDefaultBitsPerKey.
  // Every query returns false. (An SSTable never stores an empty filter; see §9.)
  BloomFilter();

  // §7.1 hash functions, exposed so callers can keep one u64 per key while writing (§7.4).
  static uint64_t Fnv1a64(const char* data, size_t n);
  static uint64_t Fmix64(uint64_t h);
  static uint64_t Hash(const std::string& key) { return Fmix64(Fnv1a64(key.data(), key.size())); }

  // k = clamp(bits_per_key * 69 / 100, 1, 30). Caller must pass a valid bits_per_key.
  static uint32_t ProbesForBitsPerKey(uint32_t bits_per_key);

  // num_bits = max(64, round_up_to_8(num_keys * bits_per_key)); 64 when num_keys == 0.
  // kInvalidArgument if bits_per_key is outside [1, 64] or the size would overflow.
  static Status ComputeNumBits(uint64_t num_keys, uint32_t bits_per_key, uint64_t* num_bits);

  // Builds a filter containing every hash in `key_hashes` (values from Hash()).
  // An empty vector yields an empty filter. kInvalidArgument for the same cases as
  // ComputeNumBits. On error *out is left unchanged.
  static Status Build(const std::vector<uint64_t>& key_hashes, uint32_t bits_per_key,
                      BloomFilter* out);

  // Validates a serialized filter section (§7.6) and loads it. `entry_count` is the number of
  // records in the owning SSTable and bounds num_bits. Returns kCorruption on any violation;
  // on error *out is left unchanged.
  static Status Parse(const char* data, size_t len, uint64_t entry_count, BloomFilter* out);

  bool MayContain(const std::string& key) const { return MayContainHash(Hash(key)); }
  bool MayContainHash(uint64_t key_hash) const;

  std::string Serialize() const;  // exactly SerializedSize() bytes
  size_t SerializedSize() const { return kHeaderBytes + bits_.size(); }

  uint32_t num_probes() const { return num_probes_; }
  uint64_t num_bits() const { return num_bits_; }

 private:
  BloomFilter(uint32_t num_probes, uint64_t num_bits);

  uint32_t num_probes_;
  uint64_t num_bits_;
  std::string bits_;  // num_bits_ / 8 bytes
};

}  // namespace raftkv
