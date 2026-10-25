#include "storage/bloom_filter.h"

#include <limits>
#include <utility>

namespace raftkv {
namespace {

constexpr uint64_t kU64Max = std::numeric_limits<uint64_t>::max();

void SetBit(std::string* bits, uint64_t b) {
  (*bits)[static_cast<size_t>(b >> 3)] |= static_cast<char>(1u << (b & 7));
}

bool TestBit(const std::string& bits, uint64_t b) {
  return (static_cast<unsigned char>(bits[static_cast<size_t>(b >> 3)]) >> (b & 7)) & 1u;
}

uint64_t GetU64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

void PutU64(std::string* s, uint64_t v) {
  for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

}  // namespace

BloomFilter::BloomFilter() : BloomFilter(ProbesForBitsPerKey(kDefaultBitsPerKey), kMinBits) {}

BloomFilter::BloomFilter(uint32_t num_probes, uint64_t num_bits)
    : num_probes_(num_probes), num_bits_(num_bits), bits_(static_cast<size_t>(num_bits / 8), '\0') {}

uint64_t BloomFilter::Fnv1a64(const char* data, size_t n) {
  uint64_t h = 0xcbf29ce484222325ull;
  for (size_t i = 0; i < n; ++i) {
    h ^= static_cast<unsigned char>(data[i]);
    h *= 0x100000001b3ull;  // unsigned: wraps mod 2^64
  }
  return h;
}

uint64_t BloomFilter::Fmix64(uint64_t h) {
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdull;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ull;
  h ^= h >> 33;
  return h;
}

uint32_t BloomFilter::ProbesForBitsPerKey(uint32_t bits_per_key) {
  uint32_t k = bits_per_key * 69 / 100;  // bits_per_key <= 64, so no overflow
  if (k < 1) k = 1;
  if (k > kMaxProbes) k = kMaxProbes;
  return k;
}

Status BloomFilter::ComputeNumBits(uint64_t num_keys, uint32_t bits_per_key, uint64_t* num_bits) {
  if (bits_per_key < kMinBitsPerKey || bits_per_key > kMaxBitsPerKey) return Status::kCorruption;
  if (num_keys == 0) {
    *num_bits = kMinBits;
    return Status::kOk;
  }
  if (num_keys > (kU64Max - 7) / bits_per_key) return Status::kCorruption;  // §7.4 overflow check
  const uint64_t rounded = (num_keys * bits_per_key + 7) / 8 * 8;            // cannot overflow now
  const uint64_t m = rounded < kMinBits ? kMinBits : rounded;
  if (m / 8 > std::numeric_limits<size_t>::max()) return Status::kCorruption;  // bit array must fit
  *num_bits = m;
  return Status::kOk;
}

Status BloomFilter::Build(const std::vector<uint64_t>& key_hashes, uint32_t bits_per_key,
                          BloomFilter* out) {
  uint64_t m = 0;
  Status s = ComputeNumBits(key_hashes.size(), bits_per_key, &m);
  if (s != Status::kOk) return s;
  BloomFilter f(ProbesForBitsPerKey(bits_per_key), m);
  for (uint64_t h : key_hashes) {
    const uint64_t h1 = h & 0xFFFFFFFFull;
    const uint64_t h2 = (h >> 32) | 1;
    for (uint32_t i = 0; i < f.num_probes_; ++i) SetBit(&f.bits_, (h1 + i * h2) % m);  // < 2^37
  }
  *out = std::move(f);
  return Status::kOk;
}

Status BloomFilter::Parse(const char* data, size_t len, uint64_t entry_count, BloomFilter* out) {
  // §7.6, in order. Every check runs before any allocation or access past the header.
  if (data == nullptr || len < kHeaderBytes + 8) return Status::kCorruption;  // filter_len >= 20
  const uint8_t hash_kind = static_cast<uint8_t>(data[0]);
  const uint8_t num_probes = static_cast<uint8_t>(data[1]);
  if (hash_kind != kHashKind) return Status::kCorruption;
  if (num_probes < 1 || num_probes > kMaxProbes) return Status::kCorruption;
  if (data[2] != 0 || data[3] != 0) return Status::kCorruption;  // reserved u16
  const uint64_t num_bits = GetU64(data + 4);
  if (num_bits % 8 != 0 || num_bits < kMinBits) return Status::kCorruption;
  if (num_bits / 8 != len - kHeaderBytes) return Status::kCorruption;  // division form: no overflow
  const uint64_t words = num_bits / 64 + (num_bits % 64 != 0 ? 1 : 0);
  if (words > (entry_count < 1 ? 1 : entry_count)) return Status::kCorruption;

  BloomFilter f(num_probes, num_bits);
  f.bits_.assign(data + kHeaderBytes, len - kHeaderBytes);
  *out = std::move(f);
  return Status::kOk;
}

bool BloomFilter::MayContainHash(uint64_t key_hash) const {
  const uint64_t h1 = key_hash & 0xFFFFFFFFull;
  const uint64_t h2 = (key_hash >> 32) | 1;
  for (uint32_t i = 0; i < num_probes_; ++i) {
    if (!TestBit(bits_, (h1 + i * h2) % num_bits_)) return false;
  }
  return true;
}

std::string BloomFilter::Serialize() const {
  std::string out;
  out.reserve(SerializedSize());
  out.push_back(static_cast<char>(kHashKind));
  out.push_back(static_cast<char>(num_probes_));
  out.push_back('\0');  // reserved u16
  out.push_back('\0');
  PutU64(&out, num_bits_);
  out += bits_;
  return out;
}

}  // namespace raftkv
