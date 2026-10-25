#include "storage/bloom_filter.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <vector>

using raftkv::BloomFilter;
using raftkv::Status;

namespace {

std::vector<uint64_t> Hashes(const std::vector<std::string>& keys) {
  std::vector<uint64_t> h;
  for (const auto& k : keys) h.push_back(BloomFilter::Hash(k));
  return h;
}

BloomFilter BuildOrDie(const std::vector<std::string>& keys, uint32_t bpk = 10) {
  BloomFilter f;
  EXPECT_EQ(BloomFilter::Build(Hashes(keys), bpk, &f), Status::kOk);
  return f;
}

Status ParseStr(const std::string& s, uint64_t entry_count, BloomFilter* out) {
  return BloomFilter::Parse(s.data(), s.size(), entry_count, out);
}

// Valid serialized filter: 64 bits, 6 probes, every bit set (so any change is detectable).
std::string ValidSection() {
  std::string s = {1, 6, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0};
  s.append(8, '\xff');
  return s;
}

void SetU64(std::string* s, size_t pos, uint64_t v) {
  for (int i = 0; i < 8; ++i) (*s)[pos + i] = static_cast<char>((v >> (8 * i)) & 0xFF);
}

struct Vector {
  std::string key;
  uint64_t fnv, hash;
  std::vector<uint64_t> probes;  // first 6 probes for m = 64
};

const std::vector<Vector>& AppendixB() {  // docs/sstable-format.md Appendix B.1
  static const std::vector<Vector> v = {
      {std::string(), 0xcbf29ce484222325ull, 0xefd01f60ba992926ull, {38, 7, 40, 9, 42, 11}},
      {"a", 0xaf63dc4c8601ec8cull, 0x82a2a958a9bece5bull, {27, 52, 13, 38, 63, 24}},
      {"raftkv", 0xb262c55919df192bull, 0x8147b66b2790f68dull, {13, 56, 35, 14, 57, 36}},
      {std::string("a\0b", 3), 0xe5d29919042666b2ull, 0xab78f5eca36d0e2bull, {43, 24, 5, 50, 31, 12}},
  };
  return v;
}

}  // namespace

TEST(BloomFilterTest, HashMatchesAppendixB) {
  for (const auto& v : AppendixB()) {
    EXPECT_EQ(BloomFilter::Fnv1a64(v.key.data(), v.key.size()), v.fnv) << v.key.size();
    EXPECT_EQ(BloomFilter::Fmix64(v.fnv), v.hash);
    EXPECT_EQ(BloomFilter::Hash(v.key), v.hash);
  }
}

TEST(BloomFilterTest, ProbeBitsMatchAppendixB) {
  // One key, bits_per_key 10: num_bits = 64 and num_probes = 6, so the serialized bit array
  // must contain exactly the Appendix B probe positions, stored LSB-first (§7.3).
  for (const auto& v : AppendixB()) {
    BloomFilter f = BuildOrDie({v.key});
    ASSERT_EQ(f.num_bits(), 64u);
    ASSERT_EQ(f.num_probes(), 6u);
    std::string expected(8, '\0');
    for (uint64_t b : v.probes) expected[b >> 3] |= static_cast<char>(1u << (b & 7));
    EXPECT_EQ(f.Serialize().substr(BloomFilter::kHeaderBytes), expected) << v.key.size();
    EXPECT_TRUE(f.MayContain(v.key));
  }
}

TEST(BloomFilterTest, SerializedHeaderLayout) {
  BloomFilter f = BuildOrDie({"a"});
  std::string s = f.Serialize();
  ASSERT_EQ(s.size(), 20u);
  EXPECT_EQ(s.substr(0, 12), std::string({1, 6, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0}));
}

TEST(BloomFilterTest, ProbeCount) {
  EXPECT_EQ(BloomFilter::ProbesForBitsPerKey(1), 1u);   // 0 clamps to 1
  EXPECT_EQ(BloomFilter::ProbesForBitsPerKey(2), 1u);
  EXPECT_EQ(BloomFilter::ProbesForBitsPerKey(3), 2u);
  EXPECT_EQ(BloomFilter::ProbesForBitsPerKey(10), 6u);  // default
  EXPECT_EQ(BloomFilter::ProbesForBitsPerKey(43), 29u);
  EXPECT_EQ(BloomFilter::ProbesForBitsPerKey(44), 30u);
  EXPECT_EQ(BloomFilter::ProbesForBitsPerKey(64), 30u);  // 44 clamps to 30
}

TEST(BloomFilterTest, NumBitsSizing) {
  uint64_t m = 0;
  ASSERT_EQ(BloomFilter::ComputeNumBits(0, 10, &m), Status::kOk);
  EXPECT_EQ(m, 64u);
  ASSERT_EQ(BloomFilter::ComputeNumBits(1, 10, &m), Status::kOk);
  EXPECT_EQ(m, 64u);  // minimum
  ASSERT_EQ(BloomFilter::ComputeNumBits(7, 10, &m), Status::kOk);
  EXPECT_EQ(m, 72u);  // 70 rounded up to a multiple of 8
  ASSERT_EQ(BloomFilter::ComputeNumBits(100, 10, &m), Status::kOk);
  EXPECT_EQ(m, 1000u);
  ASSERT_EQ(BloomFilter::ComputeNumBits(3, 64, &m), Status::kOk);
  EXPECT_EQ(m, 192u);
}

TEST(BloomFilterTest, NumBitsRejectsInvalidBitsPerKeyAndOverflow) {
  uint64_t m = 12345;
  EXPECT_EQ(BloomFilter::ComputeNumBits(10, 0, &m), Status::kCorruption);
  EXPECT_EQ(BloomFilter::ComputeNumBits(10, 65, &m), Status::kCorruption);
  const uint64_t max = std::numeric_limits<uint64_t>::max();
  // §7.4: n <= (2^64 - 1 - 7) / bits_per_key, checked before multiplying.
  EXPECT_EQ(BloomFilter::ComputeNumBits((max - 7) / 10 + 1, 10, &m), Status::kCorruption);
  EXPECT_EQ(BloomFilter::ComputeNumBits(uint64_t{1} << 58, 64, &m), Status::kCorruption);
  EXPECT_EQ(BloomFilter::ComputeNumBits(max, 1, &m), Status::kCorruption);
  EXPECT_EQ(m, 12345u) << "output untouched on error";
  if (sizeof(size_t) >= 8) {  // the bit array fits in memory on 64-bit platforms
    ASSERT_EQ(BloomFilter::ComputeNumBits((max - 7) / 10, 10, &m), Status::kOk);
    EXPECT_EQ(m % 8, 0u);
    EXPECT_GE(m, (max - 7) / 10 * 10);
  }
}

TEST(BloomFilterTest, BuildRejectsInvalidBitsPerKey) {
  BloomFilter f = BuildOrDie({"x"});
  const std::string before = f.Serialize();
  EXPECT_EQ(BloomFilter::Build(Hashes({"a"}), 0, &f), Status::kCorruption);
  EXPECT_EQ(BloomFilter::Build(Hashes({"a"}), 65, &f), Status::kCorruption);
  EXPECT_EQ(f.Serialize(), before) << "output untouched on error";
}

TEST(BloomFilterTest, EmptyFilterContainsNothing) {
  BloomFilter defaulted;
  BloomFilter built = BuildOrDie({});
  for (const BloomFilter* f : {&defaulted, &built}) {
    EXPECT_EQ(f->num_bits(), 64u);
    EXPECT_EQ(f->num_probes(), 6u);
    EXPECT_EQ(f->SerializedSize(), 20u);
    for (const auto& v : AppendixB()) EXPECT_FALSE(f->MayContain(v.key));
  }
  EXPECT_EQ(defaulted.Serialize(), built.Serialize());
}

TEST(BloomFilterTest, NoFalseNegatives) {
  std::vector<std::string> keys = {std::string(), std::string("\0", 1), std::string("\0\0", 2),
                                   std::string("a\0b", 3), std::string("\xff\x00\x7f", 3)};
  for (int i = 0; i < 20000; ++i) keys.push_back("key" + std::to_string(i));
  for (uint32_t bpk : {1u, 4u, 10u, 64u}) {
    BloomFilter f = BuildOrDie(keys, bpk);
    BloomFilter parsed;
    ASSERT_EQ(ParseStr(f.Serialize(), keys.size(), &parsed), Status::kOk);
    for (const auto& k : keys) {
      ASSERT_TRUE(f.MayContain(k)) << "bpk=" << bpk << " key size " << k.size();
      ASSERT_TRUE(parsed.MayContain(k)) << "bpk=" << bpk;
    }
  }
}

TEST(BloomFilterTest, DeterministicAndOrderIndependent) {
  std::vector<std::string> keys;
  for (int i = 0; i < 500; ++i) keys.push_back("k" + std::to_string(i));
  std::vector<std::string> reversed(keys.rbegin(), keys.rend());
  EXPECT_EQ(BuildOrDie(keys).Serialize(), BuildOrDie(keys).Serialize());
  EXPECT_EQ(BuildOrDie(keys).Serialize(), BuildOrDie(reversed).Serialize());
}

TEST(BloomFilterTest, SerializationRoundTrip) {
  std::vector<std::string> keys;
  for (int i = 0; i < 1000; ++i) keys.push_back("rt" + std::to_string(i));
  BloomFilter f = BuildOrDie(keys);
  const std::string s = f.Serialize();
  ASSERT_EQ(s.size(), f.SerializedSize());
  ASSERT_EQ(s.size(), 12u + 10000u / 8);
  BloomFilter g;
  ASSERT_EQ(ParseStr(s, keys.size(), &g), Status::kOk);
  EXPECT_EQ(g.num_bits(), f.num_bits());
  EXPECT_EQ(g.num_probes(), f.num_probes());
  EXPECT_EQ(g.Serialize(), s);
  for (int i = 0; i < 5000; ++i) {
    std::string probe = "other" + std::to_string(i);
    EXPECT_EQ(g.MayContain(probe), f.MayContain(probe));
  }
}

TEST(BloomFilterTest, FalsePositiveRateIsReasonable) {
  // 10 bits/key and 6 probes give a theoretical rate of about 0.84%. The bound is deliberately
  // loose; the hash is fixed, so the observed count is deterministic.
  std::vector<std::string> keys;
  for (int i = 0; i < 10000; ++i) keys.push_back("present-" + std::to_string(i));
  BloomFilter f = BuildOrDie(keys);
  int fp = 0;
  const int kQueries = 100000;
  for (int i = 0; i < kQueries; ++i) fp += f.MayContain("absent-" + std::to_string(i)) ? 1 : 0;
  EXPECT_LT(fp, kQueries * 2 / 100) << "false positives: " << fp;
}

TEST(BloomFilterTest, ParseAcceptsValidSection) {
  BloomFilter f;
  ASSERT_EQ(ParseStr(ValidSection(), 1, &f), Status::kOk);
  EXPECT_EQ(f.num_bits(), 64u);
  EXPECT_EQ(f.num_probes(), 6u);
  EXPECT_TRUE(f.MayContain("anything"));  // all bits set
  EXPECT_EQ(ParseStr(ValidSection(), 0, &f), Status::kOk);  // max(1, entry_count) allows 64 bits
}

TEST(BloomFilterTest, ParseRejectsTruncationAndNull) {
  const std::string s = BuildOrDie({"a", "b", "c"}).Serialize();
  BloomFilter f;
  for (size_t len = 0; len < s.size(); ++len) {
    EXPECT_EQ(BloomFilter::Parse(s.data(), len, 3, &f), Status::kCorruption) << "len=" << len;
  }
  EXPECT_EQ(BloomFilter::Parse(nullptr, 0, 3, &f), Status::kCorruption);
  EXPECT_EQ(ParseStr(s + '\0', 3, &f), Status::kCorruption) << "trailing byte";
}

TEST(BloomFilterTest, ParseRejectsBadHeaderFields) {
  BloomFilter f;
  std::string s = ValidSection();
  for (int kind : {0, 2, 255}) {
    s = ValidSection();
    s[0] = static_cast<char>(kind);
    EXPECT_EQ(ParseStr(s, 1, &f), Status::kCorruption) << "hash_kind=" << kind;
  }
  for (int k : {0, 31, 255}) {
    s = ValidSection();
    s[1] = static_cast<char>(k);
    EXPECT_EQ(ParseStr(s, 1, &f), Status::kCorruption) << "num_probes=" << k;
  }
  for (int k : {1, 30}) {
    s = ValidSection();
    s[1] = static_cast<char>(k);
    EXPECT_EQ(ParseStr(s, 1, &f), Status::kOk) << "num_probes=" << k;
  }
  for (size_t pos : {2u, 3u}) {
    s = ValidSection();
    s[pos] = 1;
    EXPECT_EQ(ParseStr(s, 1, &f), Status::kCorruption) << "reserved byte " << pos;
  }
}

TEST(BloomFilterTest, ParseRejectsBadBitCounts) {
  BloomFilter f;
  const uint64_t max = std::numeric_limits<uint64_t>::max();
  // With an 8-byte bit array, only num_bits == 64 is consistent.
  for (uint64_t bits : {uint64_t{0}, uint64_t{56}, uint64_t{63}, uint64_t{65}, uint64_t{72},
                        uint64_t{128}, max, max - 7, uint64_t{1} << 63}) {
    std::string s = ValidSection();
    SetU64(&s, 4, bits);
    EXPECT_EQ(ParseStr(s, max, &f), Status::kCorruption) << "num_bits=" << bits;
  }
  // num_bits that is a multiple of 8 and >= 64 but disagrees with the section length.
  std::string longer = ValidSection() + std::string(8, '\0');  // 16-byte array
  EXPECT_EQ(ParseStr(longer, 2, &f), Status::kCorruption);     // still claims 64 bits
  SetU64(&longer, 4, 128);
  EXPECT_EQ(ParseStr(longer, 2, &f), Status::kOk);
}

TEST(BloomFilterTest, ParseBoundsBitsByEntryCount) {
  // §7.6 step 4: ceil(num_bits / 64) <= max(1, entry_count).
  BloomFilter f;
  std::string s = {1, 6, 0, 0, static_cast<char>(136), 0, 0, 0, 0, 0, 0, 0};  // 136 bits
  s.append(17, '\0');
  EXPECT_EQ(ParseStr(s, 2, &f), Status::kCorruption);  // ceil(136/64) = 3 > 2
  EXPECT_EQ(ParseStr(s, 3, &f), Status::kOk);
  // A filter built with the maximum bits_per_key always satisfies the bound.
  std::vector<std::string> keys = {"a", "b", "c"};
  const std::string built = BuildOrDie(keys, 64).Serialize();
  EXPECT_EQ(ParseStr(built, keys.size(), &f), Status::kOk);
  EXPECT_EQ(ParseStr(built, keys.size() - 1, &f), Status::kCorruption);
}

TEST(BloomFilterTest, ParseLeavesOutputUntouchedOnError) {
  BloomFilter f = BuildOrDie({"keep"});
  const std::string before = f.Serialize();
  std::string bad = ValidSection();
  bad[0] = 9;
  EXPECT_EQ(ParseStr(bad, 1, &f), Status::kCorruption);
  EXPECT_EQ(f.Serialize(), before);
  EXPECT_TRUE(f.MayContain("keep"));
}
