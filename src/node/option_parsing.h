// Strict parsing of numeric command-line values for raftkv_node.
#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

namespace raftkv {

// Parses a non-negative decimal integer made only of the digits 0-9. Rejects (returns false and
// leaves *out unchanged): null or empty text, a sign ("-1", "+1"), spaces, a trailing unit or any
// other character ("10k", "1e6", "0x10", "1.5"), and a value that does not fit in a size_t.
// strtoull would accept several of these (it wraps "-1" to 2^64-1 and clamps overflow), which is
// the wrong behaviour for a memory size.
inline bool ParseByteCount(const char* text, size_t* out) {
  if (text == nullptr || *text == '\0') return false;
  uint64_t value = 0;
  constexpr uint64_t kMax = std::numeric_limits<size_t>::max();
  for (const char* p = text; *p != '\0'; ++p) {
    if (*p < '0' || *p > '9') return false;
    const uint64_t digit = static_cast<uint64_t>(*p - '0');
    if (value > (kMax - digit) / 10) return false;  // value * 10 + digit would exceed kMax
    value = value * 10 + digit;
  }
  *out = static_cast<size_t>(value);
  return true;
}

}  // namespace raftkv
