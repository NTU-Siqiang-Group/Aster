#pragma once

#include <stdint.h>

#include <algorithm>
#include <boost/function.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>
#include <boost/range.hpp>
#include <boost/utility.hpp>
#include <stdexcept>
#include <vector>

namespace ROCKSDB_NAMESPACE {

// typedef uint64_t uint64_t;
/* #undef SUCCINCT_USE_LIBCXX */
#ifndef SUCCINCT_USE_LIBCXX
#define SUCCINCT_USE_LIBCXX 0
#endif

/* #undef SUCCINCT_USE_INTRINSICS */
#ifndef SUCCINCT_USE_INTRINSICS
#define SUCCINCT_USE_INTRINSICS 0
#endif

/* #undef SUCCINCT_USE_POPCNT */
#ifndef SUCCINCT_USE_POPCNT
#define SUCCINCT_USE_POPCNT 0
#endif

#define QS_LIKELY(x) __builtin_expect(!!(x), 1)
#define QS_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define QS_NOINLINE __attribute__((noinline))
#define QS_ALWAYSINLINE __attribute__((always_inline))
#if defined(__GNUC__) && !defined(__clang__)
#define QS_FLATTEN_FUNC __attribute__((always_inline, flatten))
#else
#define QS_FLATTEN_FUNC QS_ALWAYSINLINE
#endif

#if SUCCINCT_USE_INTRINSICS
#include <xmmintrin.h>

#if defined(__GNUC__) || defined(__clang__)
#define __INTRIN_INLINE inline __attribute__((__always_inline__))
#elif defined(_MSC_VER)
#define __INTRIN_INLINE inline __forceinline
#else
#define __INTRIN_INLINE inline
#endif

#endif

#if SUCCINCT_USE_POPCNT
#if !SUCCINCT_USE_INTRINSICS
#error "Intrinsics support needed for popcnt"
#endif
#include <smmintrin.h>
#endif

namespace intrinsics {

#if SUCCINCT_USE_INTRINSICS

__INTRIN_INLINE uint64_t byteswap64(uint64_t value) {
#if defined(__GNUC__) || defined(__clang__)
  return __builtin_bswap64(value);
#elif defined(_MSC_VER)
  return _byteswap_uint64(value);
#else
#error Unsupported platform
#endif
}

__INTRIN_INLINE bool bsf64(unsigned long* const index, const uint64_t mask) {
#if defined(__GNUC__) || defined(__clang__)
  if (mask) {
    *index = (unsigned long)__builtin_ctzll(mask);
    return true;
  } else {
    return false;
  }
#elif defined(_MSC_VER)
  return _BitScanForward64(index, mask) != 0;
#else
#error Unsupported platform
#endif
}

__INTRIN_INLINE bool bsr64(unsigned long* const index, const uint64_t mask) {
#if defined(__GNUC__) || defined(__clang__)
  if (mask) {
    *index = (unsigned long)(63 - __builtin_clzll(mask));
    return true;
  } else {
    return false;
  }
#elif defined(_MSC_VER)
  return _BitScanReverse64(index, mask) != 0;
#else
#error Unsupported platform
#endif
}

template <typename T>
__INTRIN_INLINE void prefetch(T const* ptr) {
  _mm_prefetch((const char*)ptr, _MM_HINT_T0);
}

#else /* SUCCINCT_USE_INTRINSICS */

template <typename T>
inline void prefetch(T const* /* ptr */) {
  /* do nothing */
}

#endif /* SUCCINCT_USE_INTRINSICS */

#if SUCCINCT_USE_POPCNT

__INTRIN_INLINE uint64_t popcount(uint64_t x) {
  return uint64_t(_mm_popcnt_u64(x));
}

#endif /* SUCCINCT_USE_POPCNT */
}  // namespace intrinsics

struct global_parameters {
  global_parameters()
      : ef_log_sampling0(9),
        ef_log_sampling1(8),
        rb_log_rank1_sampling(9),
        rb_log_sampling1(8),
        log_partition_size(7) {}

  template <typename Visitor>
  void map(Visitor& visit) {
    visit(ef_log_sampling0, "ef_log_sampling0")(
        ef_log_sampling1, "ef_log_sampling1")(rb_log_rank1_sampling,
                                              "rb_log_rank1_sampling")(
        rb_log_sampling1, "rb_log_sampling1")(log_partition_size,
                                              "log_partition_size");
  }

  uint8_t ef_log_sampling0;
  uint8_t ef_log_sampling1;
  uint8_t rb_log_rank1_sampling;
  uint8_t rb_log_sampling1;
  uint8_t log_partition_size;
};

namespace tables {

const uint8_t select_in_byte[2048] = {
    8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3,
    0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0,
    1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1,
    0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0,
    2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2,
    0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0,
    1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1,
    0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0,
    3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5,
    0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0,
    1, 0, 2, 0, 1, 0, 8, 8, 8, 1, 8, 2, 2, 1, 8, 3, 3, 1, 3, 2, 2, 1, 8, 4, 4,
    1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1,
    3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 6, 6, 1, 6,
    2, 2, 1, 6, 3, 3, 1, 3, 2, 2, 1, 6, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2,
    2, 1, 6, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2,
    1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 7, 7, 1, 7, 2, 2, 1, 7, 3, 3, 1, 3, 2, 2, 1,
    7, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 7, 5, 5, 1, 5, 2, 2, 1, 5,
    3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 7, 6,
    6, 1, 6, 2, 2, 1, 6, 3, 3, 1, 3, 2, 2, 1, 6, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3,
    1, 3, 2, 2, 1, 6, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1,
    4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 8, 8, 8, 8, 8, 8, 2, 8, 8, 8, 3, 8,
    3, 3, 2, 8, 8, 8, 4, 8, 4, 4, 2, 8, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 5, 8, 5,
    5, 2, 8, 5, 5, 3, 5, 3, 3, 2, 8, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3,
    2, 8, 8, 8, 6, 8, 6, 6, 2, 8, 6, 6, 3, 6, 3, 3, 2, 8, 6, 6, 4, 6, 4, 4, 2,
    6, 4, 4, 3, 4, 3, 3, 2, 8, 6, 6, 5, 6, 5, 5, 2, 6, 5, 5, 3, 5, 3, 3, 2, 6,
    5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 7, 8, 7, 7, 2, 8, 7,
    7, 3, 7, 3, 3, 2, 8, 7, 7, 4, 7, 4, 4, 2, 7, 4, 4, 3, 4, 3, 3, 2, 8, 7, 7,
    5, 7, 5, 5, 2, 7, 5, 5, 3, 5, 3, 3, 2, 7, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3,
    4, 3, 3, 2, 8, 7, 7, 6, 7, 6, 6, 2, 7, 6, 6, 3, 6, 3, 3, 2, 7, 6, 6, 4, 6,
    4, 4, 2, 6, 4, 4, 3, 4, 3, 3, 2, 7, 6, 6, 5, 6, 5, 5, 2, 6, 5, 5, 3, 5, 3,
    3, 2, 6, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 3, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 4, 8, 4, 4, 3,
    8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 3, 8, 8, 8, 5, 8, 5, 5, 4, 8,
    5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 3, 8, 8,
    8, 6, 8, 6, 6, 4, 8, 6, 6, 4, 6, 4, 4, 3, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6,
    5, 6, 5, 5, 3, 8, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 8,
    8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 3, 8, 8, 8, 7, 8, 7, 7, 4, 8, 7, 7, 4, 7,
    4, 4, 3, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 3, 8, 7, 7, 5, 7, 5,
    5, 4, 7, 5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6,
    3, 8, 7, 7, 6, 7, 6, 6, 4, 7, 6, 6, 4, 6, 4, 4, 3, 8, 7, 7, 6, 7, 6, 6, 5,
    7, 6, 6, 5, 6, 5, 5, 3, 7, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8,
    8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 4, 8, 8, 8, 8, 8,
    8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5,
    5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8,
    7, 8, 8, 8, 7, 8, 7, 7, 4, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5,
    8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 7, 8,
    8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 4, 8, 8,
    8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6,
    5, 6, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6,
    8, 8, 8, 6, 8, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7,
    8, 7, 7, 6, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8,
    7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7};

}

namespace util {

inline void trim_newline_chars(std::string& s) {
  size_t l = s.size();
  while (l && (s[l - 1] == '\r' || s[l - 1] == '\n')) {
    --l;
  }
  s.resize(l);
}

// this is considerably faster than std::getline
inline bool fast_getline(std::string& line, FILE* input = stdin,
                         bool trim_newline = false) {
  line.clear();
  static const size_t max_buffer = 65536;
  char buffer[max_buffer];
  bool done = false;
  while (!done) {
    if (!fgets(buffer, max_buffer, input)) {
      if (!line.size()) {
        return false;
      } else {
        done = true;
      }
    }
    line += buffer;
    if (*line.rbegin() == '\n') {
      done = true;
    }
  }
  if (trim_newline) {
    trim_newline_chars(line);
  }
  return true;
}

class line_iterator
    : public boost::iterator_facade<line_iterator, std::string const,
                                    boost::forward_traversal_tag> {
 public:
  line_iterator() : m_file(0) {}

  explicit line_iterator(FILE* input, bool trim_newline = false)
      : m_file(input), m_trim_newline(trim_newline) {}

 private:
  friend class boost::iterator_core_access;

  void increment() {
    assert(m_file);
    if (!fast_getline(m_line, m_file, m_trim_newline)) {
      m_file = 0;
    }
  }

  bool equal(line_iterator const& other) const {
    return this->m_file == other.m_file;
  }

  std::string const& dereference() const { return m_line; }

  std::string m_line;
  FILE* m_file;
  bool m_trim_newline;
};

typedef std::pair<line_iterator, line_iterator> line_range_t;

inline line_range_t lines(FILE* ifs, bool trim_newline = false) {
  return std::make_pair(line_iterator(ifs, trim_newline), line_iterator());
}

struct auto_file {
  auto_file(const char* name, const char* mode = "rb") : m_file(0) {
    m_file = fopen(name, mode);
    if (!m_file) {
      std::string msg("Unable to open file '");
      msg += name;
      msg += "'.";
      throw std::invalid_argument(msg);
    }
  }

  ~auto_file() {
    if (m_file) {
      fclose(m_file);
    }
  }

  FILE* get() { return m_file; }

 private:
  auto_file();
  auto_file(const auto_file&);
  auto_file& operator=(const auto_file&);

  FILE* m_file;
};

typedef std::pair<const uint8_t*, const uint8_t*> char_range;

struct identity_adaptor {
  char_range operator()(char_range s) const { return s; }
};

struct stl_string_adaptor {
  char_range operator()(std::string const& s) const {
    const uint8_t* buf = reinterpret_cast<const uint8_t*>(s.c_str());
    const uint8_t* end = buf + s.size() + 1;  // add the null terminator
    return char_range(buf, end);
  }
};

class buffer_line_iterator
    : public boost::iterator_facade<buffer_line_iterator, std::string const,
                                    boost::forward_traversal_tag> {
 public:
  buffer_line_iterator() : m_buffer(0), m_end(0), m_cur_pos(0) {}

  buffer_line_iterator(const char* buffer, size_t size)
      : m_buffer(buffer), m_end(buffer + size), m_cur_pos(buffer) {
    (void)m_buffer;
    increment();
  }

 private:
  friend class boost::iterator_core_access;

  void increment() {
    assert(m_cur_pos);
    if (m_cur_pos >= m_end) {
      m_cur_pos = 0;
      return;
    }
    const char* begin = m_cur_pos;
    while (m_cur_pos < m_end && *m_cur_pos != '\n') {
      ++m_cur_pos;
    }
    const char* end = m_cur_pos;
    ++m_cur_pos;  // skip the newline

    if (begin != end && *(end - 1) == '\r') {
      --end;
    }
    m_cur_value = std::string(begin, size_t(end - begin));
  }

  bool equal(buffer_line_iterator const& other) const {
    return m_cur_pos == other.m_cur_pos;
  }

  std::string const& dereference() const {
    assert(m_cur_pos);
    return m_cur_value;
  }

  const char* m_buffer;
  const char* m_end;
  const char* m_cur_pos;
  std::string m_cur_value;
};

struct mmap_lines {
  typedef buffer_line_iterator iterator;
  typedef buffer_line_iterator const_iterator;

  mmap_lines(std::string filename) : m_map(filename) {}

  const_iterator begin() const {
    return const_iterator(m_map.data(), m_map.size());
  }

  const_iterator end() const { return const_iterator(); }

 private:
  boost::iostreams::mapped_file_source m_map;
};

struct input_error : std::invalid_argument {
  input_error(std::string const& what) : invalid_argument(what) {}
};

template <typename T>
inline void dispose(T& t) {
  T().swap(t);
}

inline uint64_t int2nat(int64_t x) {
  if (x < 0) {
    return uint64_t(-2 * x - 1);
  } else {
    return uint64_t(2 * x);
  }
}

inline int64_t nat2int(uint64_t n) {
  if (n % 2) {
    return -int64_t((n + 1) / 2);
  } else {
    return int64_t(n / 2);
  }
}

template <typename IntType1, typename IntType2>
inline IntType1 ceil_div(IntType1 dividend, IntType2 divisor) {
  // XXX(ot): put some static check that IntType1 >= IntType2
  IntType1 d = IntType1(divisor);
  return IntType1(dividend + d - 1) / d;
}

}  // namespace util

namespace broadword {

static const uint64_t ones_step_4 = 0x1111111111111111ULL;
static const uint64_t ones_step_8 = 0x0101010101010101ULL;
static const uint64_t ones_step_9 = 1ULL << 0 | 1ULL << 9 | 1ULL << 18 |
                                    1ULL << 27 | 1ULL << 36 | 1ULL << 45 |
                                    1ULL << 54;
static const uint64_t msbs_step_8 = 0x80ULL * ones_step_8;
static const uint64_t msbs_step_9 = 0x100ULL * ones_step_9;
static const uint64_t incr_step_8 =
    0x80ULL << 56 | 0x40ULL << 48 | 0x20ULL << 40 | 0x10ULL << 32 |
    0x8ULL << 24 | 0x4ULL << 16 | 0x2ULL << 8 | 0x1;
static const uint64_t inv_count_step_9 = 1ULL << 54 | 2ULL << 45 | 3ULL << 36 |
                                         4ULL << 27 | 5ULL << 18 | 6ULL << 9 |
                                         7ULL;

static const uint64_t magic_mask_1 = 0x5555555555555555ULL;
static const uint64_t magic_mask_2 = 0x3333333333333333ULL;
static const uint64_t magic_mask_3 = 0x0F0F0F0F0F0F0F0FULL;
static const uint64_t magic_mask_4 = 0x00FF00FF00FF00FFULL;
static const uint64_t magic_mask_5 = 0x0000FFFF0000FFFFULL;
static const uint64_t magic_mask_6 = 0x00000000FFFFFFFFULL;

inline uint64_t leq_step_8(uint64_t x, uint64_t y) {
  return ((((y | msbs_step_8) - (x & ~msbs_step_8)) ^ (x ^ y)) & msbs_step_8) >>
         7;
}

inline uint64_t uleq_step_8(uint64_t x, uint64_t y) {
  return (((((y | msbs_step_8) - (x & ~msbs_step_8)) ^ (x ^ y)) ^ (x & ~y)) &
          msbs_step_8) >>
         7;
}

inline uint64_t zcompare_step_8(uint64_t x) {
  return ((x | ((x | msbs_step_8) - ones_step_8)) & msbs_step_8) >> 7;
}

inline uint64_t uleq_step_9(uint64_t x, uint64_t y) {
  return (((((y | msbs_step_9) - (x & ~msbs_step_9)) | (x ^ y)) ^ (x & ~y)) &
          msbs_step_9) >>
         8;
}

inline uint64_t byte_counts(uint64_t x) {
  x = x - ((x & 0xa * ones_step_4) >> 1);
  x = (x & 3 * ones_step_4) + ((x >> 2) & 3 * ones_step_4);
  x = (x + (x >> 4)) & 0x0f * ones_step_8;
  return x;
}

inline uint64_t bytes_sum(uint64_t x) { return x * ones_step_8 >> 56; }

inline uint64_t popcount(uint64_t x) {
#if SUCCINCT_USE_POPCNT
  return intrinsics::popcount(x);
#else
  return bytes_sum(byte_counts(x));
#endif
}

inline uint64_t reverse_bytes(uint64_t x) {
#if SUCCINCT_USE_INTRINSICS
  return intrinsics::byteswap64(x);
#else
  x = ((x >> 8) & magic_mask_4) | ((x & magic_mask_4) << 8);
  x = ((x >> 16) & magic_mask_5) | ((x & magic_mask_5) << 16);
  x = ((x >> 32)) | ((x) << 32);
  return x;
#endif
}

inline uint64_t reverse_bits(uint64_t x) {
  x = ((x >> 1) & magic_mask_1) | ((x & magic_mask_1) << 1);
  x = ((x >> 2) & magic_mask_2) | ((x & magic_mask_2) << 2);
  x = ((x >> 4) & magic_mask_3) | ((x & magic_mask_3) << 4);
  return reverse_bytes(x);
}

inline uint64_t select_in_word(const uint64_t x, const uint64_t k) {
  assert(k < popcount(x));

  uint64_t byte_sums = byte_counts(x) * ones_step_8;

  const uint64_t k_step_8 = k * ones_step_8;
  const uint64_t geq_k_step_8 =
      (((k_step_8 | msbs_step_8) - byte_sums) & msbs_step_8);
#if SUCCINCT_USE_POPCNT
  const uint64_t place = intrinsics::popcount(geq_k_step_8) * 8;
#else
  const uint64_t place =
      ((geq_k_step_8 >> 7) * ones_step_8 >> 53) & ~uint64_t(0x7);
#endif
  const uint64_t byte_rank = k - (((byte_sums << 8) >> place) & uint64_t(0xFF));
  return place +
         tables::select_in_byte[((x >> place) & 0xFF) | (byte_rank << 8)];
}

inline uint64_t same_msb(uint64_t x, uint64_t y) { return (x ^ y) <= (x & y); }

namespace detail {
// Adapted from LSB of Chess Programming Wiki
static const uint8_t debruijn64_mapping[64] = {
    63, 0,  58, 1,  59, 47, 53, 2,  60, 39, 48, 27, 54, 33, 42, 3,
    61, 51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4,
    62, 57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21,
    56, 45, 25, 31, 35, 16, 9,  12, 44, 24, 15, 8,  23, 7,  6,  5};
static const uint64_t debruijn64 = 0x07EDD5E59A4E28C2ULL;
}  // namespace detail

// return the position of the single bit set in the word x
inline uint8_t bit_position(uint64_t x) {
  assert(popcount(x) == 1);
  return detail::debruijn64_mapping[(x * detail::debruijn64) >> 58];
}

inline uint8_t msb(uint64_t x, unsigned long& ret) {
#if SUCCINCT_USE_INTRINSICS
  return intrinsics::bsr64(&ret, x);
#else
  if (!x) {
    return false;
  }

  // right-saturate the word
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  x |= x >> 32;

  // isolate the MSB
  x ^= x >> 1;
  ret = bit_position(x);

  return true;
#endif
}

inline uint8_t msb(uint64_t x) {
  assert(x);
  unsigned long ret = -1U;
  msb(x, ret);
  return (uint8_t)ret;
}

inline uint8_t lsb(uint64_t x, unsigned long& ret) {
#if SUCCINCT_USE_INTRINSICS
  return intrinsics::bsf64(&ret, x);
#else
  if (!x) {
    return false;
  }
  ret = bit_position(x & -int64_t(x));
  return true;
#endif
}

inline uint8_t lsb(uint64_t x) {
  assert(x);
  unsigned long ret = -1U;
  lsb(x, ret);
  return (uint8_t)ret;
}

}  // namespace broadword

namespace detail {
inline size_t words_for(uint64_t n) { return util::ceil_div(n, 64); }
}  // namespace detail

class bit_vector_builder : boost::noncopyable {
 public:
  typedef std::vector<uint64_t> bits_type;

  bit_vector_builder(uint64_t size = 0, bool init = 0) : m_size(size) {
    m_bits.resize(detail::words_for(size), uint64_t(-init));
    if (size) {
      m_cur_word = &m_bits.back();
      // clear padding bits
      if (init && size % 64) {
        *m_cur_word >>= 64 - (size % 64);
      }
    }
  }

  void reserve(uint64_t size) { m_bits.reserve(detail::words_for(size)); }

  inline void encode(std::string* value) {
    size_t byte_to_fill = sizeof(m_size);
    for (size_t i = byte_to_fill - 1; i >= 0; i--) {
      value->push_back((m_size >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
    }
    if (m_size > 0) {
      byte_to_fill = sizeof(m_bits[0]);
      for (size_t m = 0; m < m_bits.size(); m++) {
        for (size_t i = byte_to_fill - 1; i >= 0; i--) {
          value->push_back((m_bits[i] >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
        }
      }
    }
  }

  inline void decode(const std::string& value, size_t prefix_length) {
    m_size = *reinterpret_cast<const u_int64_t*>(value.data() + prefix_length);
    for (size_t m = 0; m < detail::words_for(m_size); m++) {
      size_t offset = prefix_length + sizeof(u_int64_t) * (m + 1);
      m_bits.push_back(*reinterpret_cast<const u_int64_t*>(
          value.data() + prefix_length + offset));
    }
  }

  inline size_t get_offset() {
    return sizeof(u_int64_t) * (detail::words_for(m_size) + 1);
  }

  inline void push_back(bool b) {
    uint64_t pos_in_word = m_size % 64;
    if (pos_in_word == 0) {
      m_bits.push_back(0);
      m_cur_word = &m_bits.back();
    }
    *m_cur_word |= (uint64_t)b << pos_in_word;
    ++m_size;
  }

  inline void set(uint64_t pos, bool b) {
    uint64_t word = pos / 64;
    uint64_t pos_in_word = pos % 64;

    m_bits[word] &= ~(uint64_t(1) << pos_in_word);
    m_bits[word] |= uint64_t(b) << pos_in_word;
  }

  inline void set_bits(uint64_t pos, uint64_t bits, size_t len) {
    assert(pos + len <= size());
    // check there are no spurious bits
    assert(len == 64 || (bits >> len) == 0);
    if (!len) return;
    uint64_t mask = (len == 64) ? uint64_t(-1) : ((uint64_t(1) << len) - 1);
    uint64_t word = pos / 64;
    uint64_t pos_in_word = pos % 64;

    m_bits[word] &= ~(mask << pos_in_word);
    m_bits[word] |= bits << pos_in_word;

    uint64_t stored = 64 - pos_in_word;
    if (stored < len) {
      m_bits[word + 1] &= ~(mask >> stored);
      m_bits[word + 1] |= bits >> stored;
    }
  }

  inline void append_bits(uint64_t bits, size_t len) {
    // check there are no spurious bits
    assert(len == 64 || (bits >> len) == 0);
    if (!len) return;
    uint64_t pos_in_word = m_size % 64;
    m_size += len;
    if (pos_in_word == 0) {
      m_bits.push_back(bits);
    } else {
      *m_cur_word |= bits << pos_in_word;
      if (len > 64 - pos_in_word) {
        m_bits.push_back(bits >> (64 - pos_in_word));
      }
    }
    m_cur_word = &m_bits.back();
  }

  inline void zero_extend(uint64_t n) {
    m_size += n;
    uint64_t needed = detail::words_for(m_size) - m_bits.size();
    if (needed) {
      m_bits.insert(m_bits.end(), needed, 0);
      m_cur_word = &m_bits.back();
    }
  }

  inline void one_extend(uint64_t n) {
    while (n >= 64) {
      append_bits(uint64_t(-1), 64);
      n -= 64;
    }
    if (n) {
      append_bits(uint64_t(-1) >> (64 - n), n);
    }
  }

  void append(bit_vector_builder const& rhs) {
    if (!rhs.size()) return;

    uint64_t pos = m_bits.size();
    uint64_t shift = size() % 64;
    m_size = size() + rhs.size();
    m_bits.resize(detail::words_for(m_size));

    if (shift == 0) {  // word-aligned, easy case
      std::copy(rhs.m_bits.begin(), rhs.m_bits.end(),
                m_bits.begin() + ptrdiff_t(pos));
    } else {
      uint64_t* cur_word = &m_bits.front() + pos - 1;
      for (size_t i = 0; i < rhs.m_bits.size() - 1; ++i) {
        uint64_t w = rhs.m_bits[i];
        *cur_word |= w << shift;
        *++cur_word = w >> (64 - shift);
      }
      *cur_word |= rhs.m_bits.back() << shift;
      if (cur_word < &m_bits.back()) {
        *++cur_word = rhs.m_bits.back() >> (64 - shift);
      }
    }
    m_cur_word = &m_bits.back();
  }

  // reverse in place
  void reverse() {
    uint64_t shift = 64 - (size() % 64);

    uint64_t remainder = 0;
    for (size_t i = 0; i < m_bits.size(); ++i) {
      uint64_t cur_word;
      if (shift != 64) {  // this should be hoisted out
        cur_word = remainder | (m_bits[i] << shift);
        remainder = m_bits[i] >> (64 - shift);
      } else {
        cur_word = m_bits[i];
      }
      m_bits[i] = broadword::reverse_bits(cur_word);
    }
    assert(remainder == 0);
    std::reverse(m_bits.begin(), m_bits.end());
  }

  bits_type& move_bits() {
    assert(detail::words_for(m_size) == m_bits.size());
    return m_bits;
  }

  uint64_t size() const { return m_size; }

  void swap(bit_vector_builder& other) {
    m_bits.swap(other.m_bits);
    std::swap(m_size, other.m_size);
    std::swap(m_cur_word, other.m_cur_word);
  }

 private:
  bits_type m_bits;
  uint64_t m_size;
  uint64_t* m_cur_word;
};

namespace mapper {

namespace detail {
class freeze_visitor;
class map_visitor;
class sizeof_visitor;
}  // namespace detail

typedef boost::function<void()> deleter_t;

template <typename T>  // T must be a POD
class mappable_vector : boost::noncopyable {
 public:
  typedef T value_type;
  typedef const T* iterator;
  typedef const T* const_iterator;

  mappable_vector() : m_data(0), m_size(0), m_deleter() {}

  template <typename Range>
  mappable_vector(Range const& from) : m_data(0), m_size(0) {
    size_t size = boost::size(from);
    T* data = new T[size];
    m_deleter = boost::lambda::bind(boost::lambda::delete_array(), data);

    std::copy(boost::begin(from), boost::end(from), data);
    m_data = data;
    m_size = size;
  }

  ~mappable_vector() {
    if (m_deleter) {
      m_deleter();
    }
  }

  void swap(mappable_vector& other) {
    using std::swap;
    swap(m_data, other.m_data);
    swap(m_size, other.m_size);
    swap(m_deleter, other.m_deleter);
  }

  void clear() { mappable_vector().swap(*this); }

  void steal(std::vector<T>& vec) {
    clear();
    m_size = vec.size();
    if (m_size) {
      std::vector<T>* new_vec = new std::vector<T>;
      new_vec->swap(vec);
      m_deleter = boost::lambda::bind(boost::lambda::delete_ptr(), new_vec);
      m_data = &(*new_vec)[0];
    }
  }

  template <typename Range>
  void assign(Range const& from) {
    clear();
    mappable_vector(from).swap(*this);
  }

  uint64_t size() const { return m_size; }

  inline const_iterator begin() const { return m_data; }

  inline const_iterator end() const { return m_data + m_size; }

  inline T const& operator[](uint64_t i) const {
    assert(i < m_size);
    return m_data[i];
  }

  inline T const* data() const { return m_data; }

  inline void prefetch(size_t i) const { intrinsics::prefetch(m_data + i); }

  friend class detail::freeze_visitor;
  friend class detail::map_visitor;
  friend class detail::sizeof_visitor;

 protected:
  const T* m_data;
  uint64_t m_size;
  deleter_t m_deleter;
};

}  // namespace mapper

class bit_vector {
 public:
  bit_vector() : m_size(0) {}

  template <class Range>
  bit_vector(Range const& from) {
    std::vector<uint64_t> bits;
    const uint64_t first_mask = uint64_t(1);
    uint64_t mask = first_mask;
    uint64_t cur_val = 0;
    m_size = 0;
    for (typename boost::range_const_iterator<Range>::type iter =
             boost::begin(from);
         iter != boost::end(from); ++iter) {
      if (*iter) {
        cur_val |= mask;
      }
      mask <<= 1;
      m_size += 1;
      if (!mask) {
        bits.push_back(cur_val);
        mask = first_mask;
        cur_val = 0;
      }
    }
    if (mask != first_mask) {
      bits.push_back(cur_val);
    }
    m_bits.steal(bits);
  }

  bit_vector(bit_vector_builder* from) {
    m_size = from->size();
    m_bits.steal(from->move_bits());
  }

  template <typename Visitor>
  void map(Visitor& visit) {
    visit(m_size, "m_size")(m_bits, "m_bits");
  }

  void swap(bit_vector& other) {
    std::swap(other.m_size, m_size);
    other.m_bits.swap(m_bits);
  }

  inline size_t size() const { return m_size; }

  inline bool operator[](uint64_t pos) const {
    assert(pos < m_size);
    uint64_t block = pos / 64;
    assert(block < m_bits.size());
    uint64_t shift = pos % 64;
    return (m_bits[block] >> shift) & 1;
  }

  inline uint64_t get_bits(uint64_t pos, uint64_t len) const {
    assert(pos + len <= size());
    if (!len) {
      return 0;
    }
    uint64_t block = pos / 64;
    uint64_t shift = pos % 64;
    uint64_t mask = -(len == 64) | ((1ULL << len) - 1);
    if (shift + len <= 64) {
      return m_bits[block] >> shift & mask;
    } else {
      return (m_bits[block] >> shift) |
             (m_bits[block + 1] << (64 - shift) & mask);
    }
  }

  // same as get_bits(pos, 64) but it can extend further size(), padding with
  // zeros
  inline uint64_t get_word(uint64_t pos) const {
    assert(pos < size());
    uint64_t block = pos / 64;
    uint64_t shift = pos % 64;
    uint64_t word = m_bits[block] >> shift;
    if (shift && block + 1 < m_bits.size()) {
      word |= m_bits[block + 1] << (64 - shift);
    }
    return word;
  }

  // unsafe and fast version of get_word, it retrieves at least 56 bits
  inline uint64_t get_word56(uint64_t pos) const {
    // XXX check endianness?
    const char* ptr = reinterpret_cast<const char*>(m_bits.data());
    return *(reinterpret_cast<uint64_t const*>(ptr + pos / 8)) >> (pos % 8);
  }

  inline uint64_t predecessor0(uint64_t pos) const {
    assert(pos < m_size);
    uint64_t block = pos / 64;
    uint64_t shift = 64 - pos % 64 - 1;
    uint64_t word = ~m_bits[block];
    word = (word << shift) >> shift;

    unsigned long ret;
    while (!broadword::msb(word, ret)) {
      assert(block);
      word = ~m_bits[--block];
    };
    return block * 64 + ret;
  }

  inline uint64_t successor0(uint64_t pos) const {
    assert(pos < m_size);
    uint64_t block = pos / 64;
    uint64_t shift = pos % 64;
    uint64_t word = (~m_bits[block] >> shift) << shift;

    unsigned long ret;
    while (!broadword::lsb(word, ret)) {
      ++block;
      assert(block < m_bits.size());
      word = ~m_bits[block];
    };
    return block * 64 + ret;
  }

  inline uint64_t predecessor1(uint64_t pos) const {
    assert(pos < m_size);
    uint64_t block = pos / 64;
    uint64_t shift = 64 - pos % 64 - 1;
    uint64_t word = m_bits[block];
    word = (word << shift) >> shift;

    unsigned long ret;
    while (!broadword::msb(word, ret)) {
      assert(block);
      word = m_bits[--block];
    };
    return block * 64 + ret;
  }

  inline uint64_t successor1(uint64_t pos) const {
    assert(pos < m_size);
    uint64_t block = pos / 64;
    uint64_t shift = pos % 64;
    uint64_t word = (m_bits[block] >> shift) << shift;

    unsigned long ret;
    while (!broadword::lsb(word, ret)) {
      ++block;
      assert(block < m_bits.size());
      word = m_bits[block];
    };
    return block * 64 + ret;
  }

  mapper::mappable_vector<uint64_t> const& data() const { return m_bits; }

  struct enumerator {
    enumerator() : m_bv(0), m_pos(uint64_t(-1)) {}

    enumerator(bit_vector const& bv, size_t pos)
        : m_bv(&bv), m_pos(pos), m_buf(0), m_avail(0) {
      m_bv->data().prefetch(m_pos / 64);
    }

    inline bool next() {
      if (!m_avail) fill_buf();
      bool b = m_buf & 1;
      m_buf >>= 1;
      m_avail -= 1;
      m_pos += 1;
      return b;
    }

    inline uint64_t take(size_t l) {
      if (m_avail < l) fill_buf();
      uint64_t val;
      if (l != 64) {
        val = m_buf & ((uint64_t(1) << l) - 1);
        m_buf >>= l;
      } else {
        val = m_buf;
      }
      m_avail -= l;
      m_pos += l;
      return val;
    }

    inline uint64_t skip_zeros() {
      uint64_t zs = 0;
      // XXX the loop may be optimized by aligning access
      while (!m_buf) {
        m_pos += m_avail;
        zs += m_avail;
        m_avail = 0;
        fill_buf();
      }

      uint64_t l = broadword::lsb(m_buf);
      m_buf >>= l;
      m_buf >>= 1;
      m_avail -= l + 1;
      m_pos += l + 1;
      return zs + l;
    }

    inline uint64_t position() const { return m_pos; }

   private:
    inline void fill_buf() {
      m_buf = m_bv->get_word(m_pos);
      m_avail = 64;
    }

    bit_vector const* m_bv;
    size_t m_pos;
    uint64_t m_buf;
    size_t m_avail;
  };

  struct unary_enumerator {
    unary_enumerator() : m_data(0), m_position(0), m_buf(0) {}

    unary_enumerator(bit_vector const& bv, uint64_t pos) {
      m_data = bv.data().data();
      m_position = pos;
      m_buf = m_data[pos / 64];
      // clear low bits
      m_buf &= uint64_t(-1) << (pos % 64);
    }

    uint64_t position() const { return m_position; }

    uint64_t next() {
      unsigned long pos_in_word;
      uint64_t buf = m_buf;
      while (!broadword::lsb(buf, pos_in_word)) {
        m_position += 64;
        buf = m_data[m_position / 64];
      }

      m_buf = buf & (buf - 1);  // clear LSB
      m_position = (m_position & ~uint64_t(63)) + pos_in_word;
      return m_position;
    }

    // skip to the k-th one after the current position
    void skip(uint64_t k) {
      uint64_t skipped = 0;
      uint64_t buf = m_buf;
      uint64_t w = 0;
      while (skipped + (w = broadword::popcount(buf)) <= k) {
        skipped += w;
        m_position += 64;
        buf = m_data[m_position / 64];
      }
      assert(buf);
      uint64_t pos_in_word = broadword::select_in_word(buf, k - skipped);
      m_buf = buf & (uint64_t(-1) << pos_in_word);
      m_position = (m_position & ~uint64_t(63)) + pos_in_word;
    }

    // skip to the k-th zero after the current position
    void skip0(uint64_t k) {
      uint64_t skipped = 0;
      uint64_t pos_in_word = m_position % 64;
      uint64_t buf = ~m_buf & (uint64_t(-1) << pos_in_word);
      uint64_t w = 0;
      while (skipped + (w = broadword::popcount(buf)) <= k) {
        skipped += w;
        m_position += 64;
        buf = ~m_data[m_position / 64];
      }
      assert(buf);
      pos_in_word = broadword::select_in_word(buf, k - skipped);
      m_buf = ~buf & (uint64_t(-1) << pos_in_word);
      m_position = (m_position & ~uint64_t(63)) + pos_in_word;
    }

   private:
    uint64_t const* m_data;
    uint64_t m_position;
    uint64_t m_buf;
  };

 protected:
  size_t m_size;
  mapper::mappable_vector<uint64_t> m_bits;
};

inline uint64_t ceil_log2(const uint64_t x) {
  assert(x > 0);
  return (x > 1) ? broadword::msb(x - 1) + 1 : 0;
}

struct compact_elias_fano {
  struct offsets {
    offsets() {}

    offsets(uint64_t base_offset, uint64_t universe, uint64_t n,
            global_parameters const& params)
        : universe(universe),
          n(n),
          log_sampling0(params.ef_log_sampling0),
          log_sampling1(params.ef_log_sampling1)

          ,
          lower_bits(universe > n ? broadword::msb(universe / n) : 0),
          mask((uint64_t(1) << lower_bits) - 1)
          // pad with a zero on both sides as sentinels
          ,
          higher_bits_length(n + (universe >> lower_bits) + 2),
          pointer_size(ceil_log2(higher_bits_length)),
          pointers0((higher_bits_length - n) >> log_sampling0)  // XXX
          ,
          pointers1(n >> log_sampling1)

          ,
          pointers0_offset(base_offset),
          pointers1_offset(pointers0_offset + pointers0 * pointer_size),
          higher_bits_offset(pointers1_offset + pointers1 * pointer_size),
          lower_bits_offset(higher_bits_offset + higher_bits_length),
          end(lower_bits_offset + n * lower_bits) {
      assert(n > 0);
    }

    uint64_t universe;
    uint64_t n;
    uint64_t log_sampling0;
    uint64_t log_sampling1;

    uint64_t lower_bits;
    uint64_t mask;
    uint64_t higher_bits_length;
    uint64_t pointer_size;
    uint64_t pointers0;
    uint64_t pointers1;

    uint64_t pointers0_offset;
    uint64_t pointers1_offset;
    uint64_t higher_bits_offset;
    uint64_t lower_bits_offset;
    uint64_t end;
  };

  static QS_FLATTEN_FUNC uint64_t bitsize(global_parameters const& params,
                                          uint64_t universe, uint64_t n) {
    return offsets(0, universe, n, params).end;
  }

  template <typename Iterator>
  static void write(bit_vector_builder& bvb, Iterator begin, uint64_t universe,
                    uint64_t n, global_parameters const& params) {
    using util::ceil_div;
    uint64_t base_offset = bvb.size();
    offsets of(base_offset, universe, n, params);
    // initialize all the bits to 0
    bvb.zero_extend(of.end - base_offset);

    uint64_t sample1_mask = (uint64_t(1) << of.log_sampling1) - 1;
    uint64_t offset;

    // utility function to set 0 pointers
    auto set_ptr0s = [&](uint64_t begin, uint64_t end, uint64_t rank_end) {
      uint64_t begin_zeros = begin - rank_end;
      uint64_t end_zeros = end - rank_end;

      for (uint64_t ptr0 =
               ceil_div(begin_zeros, uint64_t(1) << of.log_sampling0);
           (ptr0 << of.log_sampling0) < end_zeros; ++ptr0) {
        if (!ptr0) continue;
        offset = of.pointers0_offset + (ptr0 - 1) * of.pointer_size;
        assert(offset + of.pointer_size <= of.pointers1_offset);
        bvb.set_bits(offset, (ptr0 << of.log_sampling0) + rank_end,
                     of.pointer_size);
      }
    };

    uint64_t last = 0;
    uint64_t last_high = 0;
    Iterator it = begin;
    for (size_t i = 0; i < n; ++i) {
      uint64_t v = *it++;
      if (i && v < last) {
        throw std::runtime_error("Sequence is not sorted");
      }
      assert(v < universe);
      uint64_t high = (v >> of.lower_bits) + i + 1;
      uint64_t low = v & of.mask;

      bvb.set(of.higher_bits_offset + high, 1);

      offset = of.lower_bits_offset + i * of.lower_bits;
      assert(offset + of.lower_bits <= of.end);
      bvb.set_bits(offset, low, of.lower_bits);

      if (i && (i & sample1_mask) == 0) {
        uint64_t ptr1 = i >> of.log_sampling1;
        assert(ptr1 > 0);
        offset = of.pointers1_offset + (ptr1 - 1) * of.pointer_size;
        assert(offset + of.pointer_size <= of.higher_bits_offset);
        bvb.set_bits(offset, high, of.pointer_size);
      }

      // write pointers for the run of zeros in [last_high, high)
      set_ptr0s(last_high + 1, high, i);
      last_high = high;
      last = v;
    }

    // pointers to zeros after the last 1
    set_ptr0s(last_high + 1, of.higher_bits_length, n);  // XXX
  }

  class enumerator {
   public:
    typedef std::pair<uint64_t, uint64_t> value_type;  // (position, value)

    enumerator() {}

    enumerator(bit_vector const& bv, uint64_t offset, uint64_t universe,
               uint64_t n, global_parameters const& params)
        : m_bv(&bv),
          m_of(offset, universe, n, params),
          m_position(size()),
          m_value(m_of.universe) {}

    value_type move(uint64_t position) {
      assert(position <= m_of.n);

      if (position == m_position) {
        return value();
      }

      uint64_t skip = position - m_position;
      // optimize small forward skips
      if (QS_LIKELY(position > m_position && skip <= linear_scan_threshold)) {
        m_position = position;
        if (QS_UNLIKELY(m_position == size())) {
          m_value = m_of.universe;
        } else {
          bit_vector::unary_enumerator he = m_high_enumerator;
          for (size_t i = 0; i < skip; ++i) {
            he.next();
          }
          m_value = ((he.position() - m_of.higher_bits_offset - m_position - 1)
                     << m_of.lower_bits) |
                    read_low();
          m_high_enumerator = he;
        }
        return value();
      }

      return slow_move(position);
    }

    value_type next_geq(uint64_t lower_bound) {
      if (lower_bound == m_value) {
        return value();
      }

      uint64_t high_lower_bound = lower_bound >> m_of.lower_bits;
      uint64_t cur_high = m_value >> m_of.lower_bits;
      uint64_t high_diff = high_lower_bound - cur_high;

      if (QS_LIKELY(lower_bound > m_value &&
                    high_diff <= linear_scan_threshold)) {
        // optimize small skips
        next_reader next_value(*this, m_position + 1);
        uint64_t val;
        do {
          m_position += 1;
          if (QS_LIKELY(m_position < size())) {
            val = next_value();
          } else {
            val = m_of.universe;
            break;
          }
        } while (val < lower_bound);

        m_value = val;
        return value();
      } else {
        return slow_next_geq(lower_bound);
      }
    }

    uint64_t size() const { return m_of.n; }

    value_type next() {
      m_position += 1;
      assert(m_position <= size());

      if (QS_LIKELY(m_position < size())) {
        m_value = read_next();
      } else {
        m_value = m_of.universe;
      }
      return value();
    }

    uint64_t prev_value() const {
      if (m_position == 0) {
        return 0;
      }

      uint64_t prev_high = 0;
      if (QS_LIKELY(m_position < size())) {
        prev_high = m_bv->predecessor1(m_high_enumerator.position() - 1);
      } else {
        prev_high = m_bv->predecessor1(m_of.lower_bits_offset - 1);
      }
      prev_high -= m_of.higher_bits_offset;

      uint64_t prev_pos = m_position - 1;
      uint64_t prev_low = m_bv->get_word56(m_of.lower_bits_offset +
                                           prev_pos * m_of.lower_bits) &
                          m_of.mask;
      return ((prev_high - prev_pos - 1) << m_of.lower_bits) | prev_low;
    }

    uint64_t position() const { return m_position; }

   private:
    value_type QS_NOINLINE slow_move(uint64_t position) {
      if (QS_UNLIKELY(position == size())) {
        m_position = position;
        m_value = m_of.universe;
        return value();
      }

      uint64_t skip = position - m_position;
      uint64_t to_skip;
      if (position > m_position && (skip >> m_of.log_sampling1) == 0) {
        to_skip = skip - 1;
      } else {
        uint64_t ptr = position >> m_of.log_sampling1;
        uint64_t high_pos = pointer1(ptr);
        uint64_t high_rank = ptr << m_of.log_sampling1;
        m_high_enumerator = bit_vector::unary_enumerator(
            *m_bv, m_of.higher_bits_offset + high_pos);
        to_skip = position - high_rank;
      }

      m_high_enumerator.skip(to_skip);
      m_position = position;
      m_value = read_next();
      return value();
    }

    value_type QS_NOINLINE slow_next_geq(uint64_t lower_bound) {
      if (QS_UNLIKELY(lower_bound >= m_of.universe)) {
        return move(size());
      }

      uint64_t high_lower_bound = lower_bound >> m_of.lower_bits;
      uint64_t cur_high = m_value >> m_of.lower_bits;
      uint64_t high_diff = high_lower_bound - cur_high;

      // XXX bounds checking!
      uint64_t to_skip;
      if (lower_bound > m_value && (high_diff >> m_of.log_sampling0) == 0) {
        // note: at the current position in the bitvector there
        // should be a 1, but since we already consumed it, it
        // is 0 in the enumerator, so we need to skip it
        to_skip = high_diff;
      } else {
        uint64_t ptr = high_lower_bound >> m_of.log_sampling0;
        uint64_t high_pos = pointer0(ptr);
        uint64_t high_rank0 = ptr << m_of.log_sampling0;

        m_high_enumerator = bit_vector::unary_enumerator(
            *m_bv, m_of.higher_bits_offset + high_pos);
        to_skip = high_lower_bound - high_rank0;
      }

      m_high_enumerator.skip0(to_skip);
      m_position = m_high_enumerator.position() - m_of.higher_bits_offset -
                   high_lower_bound;

      next_reader read_value(*this, m_position);
      while (true) {
        if (QS_UNLIKELY(m_position == size())) {
          m_value = m_of.universe;
          return value();
        }
        auto val = read_value();
        if (val >= lower_bound) {
          m_value = val;
          return value();
        }
        m_position++;
      }
    }

    static const uint64_t linear_scan_threshold = 8;

    inline value_type value() const { return value_type(m_position, m_value); }

    inline uint64_t read_low() {
      return m_bv->get_word56(m_of.lower_bits_offset +
                              m_position * m_of.lower_bits) &
             m_of.mask;
    }

    inline uint64_t read_next() {
      assert(m_position < size());
      uint64_t high = m_high_enumerator.next() - m_of.higher_bits_offset;
      return ((high - m_position - 1) << m_of.lower_bits) | read_low();
    }

    struct next_reader {
      next_reader(enumerator& e, uint64_t position)
          : e(e),
            high_enumerator(e.m_high_enumerator),
            high_base(e.m_of.higher_bits_offset + position + 1),
            lower_bits(e.m_of.lower_bits),
            lower_base(e.m_of.lower_bits_offset + position * lower_bits),
            mask(e.m_of.mask),
            bv(*e.m_bv) {}

      ~next_reader() { e.m_high_enumerator = high_enumerator; }

      uint64_t operator()() {
        uint64_t high = high_enumerator.next() - high_base;
        uint64_t low = bv.get_word56(lower_base) & mask;
        high_base += 1;
        lower_base += lower_bits;
        return (high << lower_bits) | low;
      }

      enumerator& e;
      bit_vector::unary_enumerator high_enumerator;
      uint64_t high_base, lower_bits, lower_base, mask;
      bit_vector const& bv;
    };

    inline uint64_t pointer(uint64_t offset, uint64_t i) const {
      if (i == 0) {
        return 0;
      } else {
        return m_bv->get_word56(offset + (i - 1) * m_of.pointer_size) &
               ((uint64_t(1) << m_of.pointer_size) - 1);
      }
    }

    inline uint64_t pointer0(uint64_t i) const {
      return pointer(m_of.pointers0_offset, i);
    }

    inline uint64_t pointer1(uint64_t i) const {
      return pointer(m_of.pointers1_offset, i);
    }

    bit_vector const* m_bv;
    offsets m_of;

    uint64_t m_position;
    uint64_t m_value;
    bit_vector::unary_enumerator m_high_enumerator;
  };
};

struct compact_ranked_bitvector {
  struct offsets {
    offsets(uint64_t base_offset, uint64_t universe, uint64_t n,
            global_parameters const& params)
        : universe(universe),
          n(n),
          log_rank1_sampling(params.rb_log_rank1_sampling),
          log_sampling1(params.rb_log_sampling1)

          ,
          rank1_sample_size(ceil_log2(n + 1)),
          pointer_size(ceil_log2(universe)),
          rank1_samples(universe >> params.rb_log_rank1_sampling),
          pointers1(n >> params.rb_log_sampling1)

          ,
          rank1_samples_offset(base_offset),
          pointers1_offset(rank1_samples_offset +
                           rank1_samples * rank1_sample_size),
          bits_offset(pointers1_offset + pointers1 * pointer_size),
          end(bits_offset + universe) {}

    uint64_t universe;
    uint64_t n;
    uint64_t log_rank1_sampling;
    uint64_t log_sampling1;

    uint64_t rank1_sample_size;
    uint64_t pointer_size;

    uint64_t rank1_samples;
    uint64_t pointers1;

    uint64_t rank1_samples_offset;
    uint64_t pointers1_offset;
    uint64_t bits_offset;
    uint64_t end;
  };

  static QS_FLATTEN_FUNC uint64_t bitsize(global_parameters const& params,
                                          uint64_t universe, uint64_t n) {
    return offsets(0, universe, n, params).end;
  }

  template <typename Iterator>
  static void write(bit_vector_builder& bvb, Iterator begin, uint64_t universe,
                    uint64_t n, global_parameters const& params) {
    using util::ceil_div;

    uint64_t base_offset = bvb.size();
    offsets of(base_offset, universe, n, params);
    // initialize all the bits to 0
    bvb.zero_extend(of.end - base_offset);

    uint64_t offset;

    auto set_rank1_samples = [&](uint64_t begin, uint64_t end, uint64_t rank) {
      for (uint64_t sample =
               ceil_div(begin, uint64_t(1) << of.log_rank1_sampling);
           (sample << of.log_rank1_sampling) < end; ++sample) {
        if (!sample) continue;
        offset = of.rank1_samples_offset + (sample - 1) * of.rank1_sample_size;
        assert(offset + of.rank1_sample_size <= of.pointers1_offset);
        bvb.set_bits(offset, rank, of.rank1_sample_size);
      }
    };

    uint64_t sample1_mask = (uint64_t(1) << of.log_sampling1) - 1;
    uint64_t last = 0;
    Iterator it = begin;
    for (size_t i = 0; i < n; ++i) {
      uint64_t v = *it++;
      if (i && v == last) {
        throw std::runtime_error("Duplicate element");
      }
      if (i && v < last) {
        throw std::runtime_error("Sequence is not sorted");
      }

      assert(!i || v > last);
      assert(v <= universe);

      bvb.set(of.bits_offset + v, 1);

      if (i && (i & sample1_mask) == 0) {
        uint64_t ptr1 = i >> of.log_sampling1;
        assert(ptr1 > 0);
        offset = of.pointers1_offset + (ptr1 - 1) * of.pointer_size;
        assert(offset + of.pointer_size <= of.bits_offset);
        bvb.set_bits(offset, v, of.pointer_size);
      }

      set_rank1_samples(last + 1, v + 1, i);
      last = v;
    }

    set_rank1_samples(last + 1, universe, n);
  }

  class enumerator {
   public:
    typedef std::pair<uint64_t, uint64_t> value_type;  // (position, value)

    enumerator(bit_vector const& bv, uint64_t offset, uint64_t universe,
               uint64_t n, global_parameters const& params)
        : m_bv(&bv),
          m_of(offset, universe, n, params),
          m_position(size()),
          m_value(m_of.universe) {}

    value_type move(uint64_t position) {
      assert(position <= size());

      if (position == m_position) {
        return value();
      }

      // optimize small forward skips
      uint64_t skip = position - m_position;
      if (QS_LIKELY(position > m_position && skip <= linear_scan_threshold)) {
        m_position = position;
        if (QS_UNLIKELY(m_position == size())) {
          m_value = m_of.universe;
        } else {
          bit_vector::unary_enumerator he = m_enumerator;
          for (size_t i = 0; i < skip; ++i) {
            he.next();
          }
          m_value = he.position() - m_of.bits_offset;
          m_enumerator = he;
        }

        return value();
      }

      return slow_move(position);
    }

    value_type next_geq(uint64_t lower_bound) {
      if (lower_bound == m_value) {
        return value();
      }

      uint64_t diff = lower_bound - m_value;
      if (QS_LIKELY(lower_bound > m_value && diff <= linear_scan_threshold)) {
        // optimize small skips
        bit_vector::unary_enumerator he = m_enumerator;
        uint64_t val;
        do {
          m_position += 1;
          if (QS_LIKELY(m_position < size())) {
            val = he.next() - m_of.bits_offset;
          } else {
            val = m_of.universe;
            break;
          }
        } while (val < lower_bound);

        m_value = val;
        m_enumerator = he;
        return value();
      } else {
        return slow_next_geq(lower_bound);
      }
    }

    value_type next() {
      m_position += 1;
      assert(m_position <= size());

      if (QS_LIKELY(m_position < size())) {
        m_value = read_next();
      } else {
        m_value = m_of.universe;
      }
      return value();
    }

    uint64_t size() const { return m_of.n; }

    uint64_t prev_value() const {
      if (m_position == 0) {
        return 0;
      }

      uint64_t pos = 0;
      if (QS_LIKELY(m_position < size())) {
        pos = m_bv->predecessor1(m_enumerator.position() - 1);
      } else {
        pos = m_bv->predecessor1(m_of.end - 1);
      }

      return pos - m_of.bits_offset;
    }

   private:
    value_type QS_NOINLINE slow_move(uint64_t position) {
      uint64_t skip = position - m_position;
      if (QS_UNLIKELY(position == size())) {
        m_position = position;
        m_value = m_of.universe;
        return value();
      }

      uint64_t to_skip;
      if (position > m_position && (skip >> m_of.log_sampling1) == 0) {
        to_skip = skip - 1;
      } else {
        uint64_t ptr = position >> m_of.log_sampling1;
        uint64_t ptr_pos = pointer1(ptr);

        m_enumerator =
            bit_vector::unary_enumerator(*m_bv, m_of.bits_offset + ptr_pos);
        to_skip = position - (ptr << m_of.log_sampling1);
      }

      m_enumerator.skip(to_skip);
      m_position = position;
      m_value = read_next();

      return value();
    }

    value_type QS_NOINLINE slow_next_geq(uint64_t lower_bound) {
      using broadword::popcount;

      if (QS_UNLIKELY(lower_bound >= m_of.universe)) {
        return move(size());
      }

      uint64_t skip = lower_bound - m_value;
      m_enumerator =
          bit_vector::unary_enumerator(*m_bv, m_of.bits_offset + lower_bound);

      uint64_t begin;
      if (lower_bound > m_value && (skip >> m_of.log_rank1_sampling) == 0) {
        begin = m_of.bits_offset + m_value;
      } else {
        uint64_t block = lower_bound >> m_of.log_rank1_sampling;
        m_position = rank1_sample(block);

        begin = m_of.bits_offset + (block << m_of.log_rank1_sampling);
      }

      uint64_t end = m_of.bits_offset + lower_bound;
      uint64_t begin_word = begin / 64;
      uint64_t begin_shift = begin % 64;
      uint64_t end_word = end / 64;
      uint64_t end_shift = end % 64;
      uint64_t word = (m_bv->data()[begin_word] >> begin_shift) << begin_shift;

      while (begin_word < end_word) {
        m_position += popcount(word);
        word = m_bv->data()[++begin_word];
      }
      if (end_shift) {
        m_position += popcount(word << (64 - end_shift));
      }

      if (m_position < size()) {
        m_value = read_next();
      } else {
        m_value = m_of.universe;
      }

      return value();
    }

    static const uint64_t linear_scan_threshold = 8;

    inline value_type value() const { return value_type(m_position, m_value); }

    inline uint64_t read_next() {
      return m_enumerator.next() - m_of.bits_offset;
    }

    inline uint64_t pointer(uint64_t offset, uint64_t i, uint64_t size) const {
      if (i == 0) {
        return 0;
      } else {
        return m_bv->get_word56(offset + (i - 1) * size) &
               ((uint64_t(1) << size) - 1);
      }
    }

    inline uint64_t pointer1(uint64_t i) const {
      return pointer(m_of.pointers1_offset, i, m_of.pointer_size);
    }

    inline uint64_t rank1_sample(uint64_t i) const {
      return pointer(m_of.rank1_samples_offset, i, m_of.rank1_sample_size);
    }

    bit_vector const* m_bv;
    offsets m_of;

    uint64_t m_position;
    uint64_t m_value;
    bit_vector::unary_enumerator m_enumerator;
  };
};

struct all_ones_sequence {
  inline static uint64_t bitsize(global_parameters const& /* params */,
                                 uint64_t universe, uint64_t n) {
    return (universe == n) ? 0 : uint64_t(-1);
  }

  template <typename Iterator>
  static void write(bit_vector_builder&, Iterator, uint64_t universe,
                    uint64_t n, global_parameters const&) {
    assert(universe == n);
    (void)universe;
    (void)n;
  }

  class enumerator {
   public:
    typedef std::pair<uint64_t, uint64_t> value_type;  // (position, value)

    enumerator(bit_vector const&, uint64_t, uint64_t universe, uint64_t n,
               global_parameters const&)
        : m_universe(universe), m_position(size()) {
      assert(universe == n);
      (void)n;
    }

    value_type move(uint64_t position) {
      assert(position <= size());
      m_position = position;
      return value_type(m_position, m_position);
    }

    value_type next_geq(uint64_t lower_bound) {
      assert(lower_bound <= size());
      m_position = lower_bound;
      return value_type(m_position, m_position);
    }

    value_type next() {
      m_position += 1;
      return value_type(m_position, m_position);
    }

    uint64_t size() const { return m_universe; }

    uint64_t prev_value() const {
      if (m_position == 0) {
        return 0;
      }
      return m_position - 1;
    }

   private:
    uint64_t m_universe;
    uint64_t m_position;
  };
};

struct indexed_sequence {
  enum index_type {
    elias_fano = 0,
    ranked_bitvector = 1,
    all_ones = 2,

    index_types = 3
  };

  static const uint64_t type_bits = 1;  // all_ones is implicit

  static QS_FLATTEN_FUNC uint64_t bitsize(global_parameters const& params,
                                          uint64_t universe, uint64_t n) {
    uint64_t best_cost = all_ones_sequence::bitsize(params, universe, n);

    uint64_t ef_cost =
        compact_elias_fano::bitsize(params, universe, n) + type_bits;
    if (ef_cost < best_cost) {
      best_cost = ef_cost;
    }

    uint64_t rb_cost =
        compact_ranked_bitvector::bitsize(params, universe, n) + type_bits;
    if (rb_cost < best_cost) {
      best_cost = rb_cost;
    }

    return best_cost;
  }

  template <typename Iterator>
  static void write(bit_vector_builder& bvb, Iterator begin, uint64_t universe,
                    uint64_t n, global_parameters const& params) {
    uint64_t best_cost = all_ones_sequence::bitsize(params, universe, n);
    int best_type = all_ones;

    if (best_cost) {
      uint64_t ef_cost =
          compact_elias_fano::bitsize(params, universe, n) + type_bits;
      if (ef_cost < best_cost) {
        best_cost = ef_cost;
        best_type = elias_fano;
      }

      uint64_t rb_cost =
          compact_ranked_bitvector::bitsize(params, universe, n) + type_bits;
      if (rb_cost < best_cost) {
        best_cost = rb_cost;
        best_type = ranked_bitvector;
      }

      bvb.append_bits(best_type, type_bits);
    }

    switch (best_type) {
      case elias_fano:
        compact_elias_fano::write(bvb, begin, universe, n, params);
        break;
      case ranked_bitvector:
        compact_ranked_bitvector::write(bvb, begin, universe, n, params);
        break;
      case all_ones:
        all_ones_sequence::write(bvb, begin, universe, n, params);
        break;
      default:
        assert(false);
    }
  }

  class enumerator {
   public:
    typedef std::pair<uint64_t, uint64_t> value_type;  // (position, value)

    enumerator() {}

    enumerator(bit_vector const& bv, uint64_t offset, uint64_t universe,
               uint64_t n, global_parameters const& params) {
      if (all_ones_sequence::bitsize(params, universe, n) == 0) {
        m_type = all_ones;
      } else {
        m_type = index_type(bv.get_word56(offset) &
                            ((uint64_t(1) << type_bits) - 1));
      }

      switch (m_type) {
        case elias_fano:
          m_ef_enumerator = compact_elias_fano::enumerator(
              bv, offset + type_bits, universe, n, params);
          break;
        case ranked_bitvector:
          m_rb_enumerator = compact_ranked_bitvector::enumerator(
              bv, offset + type_bits, universe, n, params);
          break;
        case all_ones:
          m_ao_enumerator = all_ones_sequence::enumerator(
              bv, offset + type_bits, universe, n, params);
          break;
        default:
          throw std::invalid_argument("Unsupported type");
      }
    }

#define ENUMERATOR_METHOD(RETURN_TYPE, METHOD, FORMALS, ACTUALS) \
  RETURN_TYPE QS_FLATTEN_FUNC METHOD FORMALS {                   \
    switch (__builtin_expect(m_type, elias_fano)) {              \
      case elias_fano:                                           \
        return m_ef_enumerator.METHOD ACTUALS;                   \
      case ranked_bitvector:                                     \
        return m_rb_enumerator.METHOD ACTUALS;                   \
      case all_ones:                                             \
        return m_ao_enumerator.METHOD ACTUALS;                   \
      default:                                                   \
        assert(false);                                           \
        __builtin_unreachable();                                 \
    }                                                            \
  }                                                              \
    /**/

    // semicolons are redundant but they are needed to get emacs to
    // align the lines properly
    ENUMERATOR_METHOD(value_type, move, (uint64_t position), (position));
    ENUMERATOR_METHOD(value_type, next_geq, (uint64_t lower_bound),
                      (lower_bound));
    ENUMERATOR_METHOD(value_type, next, (), ());
    ENUMERATOR_METHOD(uint64_t, size, () const, ());
    ENUMERATOR_METHOD(uint64_t, prev_value, () const, ());

#undef ENUMERATOR_METHOD
#undef ENUMERATOR_VOID_METHOD

   private:
    index_type m_type;
    union {
      compact_elias_fano::enumerator m_ef_enumerator;
      compact_ranked_bitvector::enumerator m_rb_enumerator;
      all_ones_sequence::enumerator m_ao_enumerator;
    };
  };
};

inline void write_gamma(bit_vector_builder& bvb, uint64_t n) {
  uint64_t nn = n + 1;
  uint64_t l = broadword::msb(nn);
  uint64_t hb = uint64_t(1) << l;
  bvb.append_bits(hb, l + 1);
  bvb.append_bits(nn ^ hb, l);
}

inline void write_gamma_nonzero(bit_vector_builder& bvb, uint64_t n) {
  assert(n > 0);
  write_gamma(bvb, n - 1);
}

inline uint64_t read_gamma(bit_vector::enumerator& it) {
  uint64_t l = it.skip_zeros();
  return (it.take(l) | (uint64_t(1) << l)) - 1;
}

inline uint64_t read_gamma_nonzero(bit_vector::enumerator& it) {
  return read_gamma(it) + 1;
}

inline void write_delta(bit_vector_builder& bvb, uint64_t n) {
  uint64_t nn = n + 1;
  uint64_t l = broadword::msb(nn);
  uint64_t hb = uint64_t(1) << l;
  write_gamma(bvb, l);
  bvb.append_bits(nn ^ hb, l);
}

inline uint64_t read_delta(bit_vector::enumerator& it) {
  uint64_t l = read_gamma(it);
  return (it.take(l) | (uint64_t(1) << l)) - 1;
}

template <typename BaseSequence = indexed_sequence>
struct uniform_partitioned_sequence {
  typedef BaseSequence base_sequence_type;
  typedef typename base_sequence_type::enumerator base_sequence_enumerator;

  template <typename Iterator>
  static void write(bit_vector_builder& bvb, Iterator begin, uint64_t universe,
                    uint64_t n, global_parameters const& params) {
    using util::ceil_div;
    assert(n > 0);
    uint64_t partition_size = uint64_t(1) << params.log_partition_size;
    size_t partitions = ceil_div(n, partition_size);
    write_gamma_nonzero(bvb, partitions);

    std::vector<uint64_t> cur_partition;
    uint64_t cur_base = 0;
    if (partitions == 1) {
      cur_base = *begin;
      Iterator it = begin;

      for (size_t i = 0; i < n; ++i, ++it) {
        cur_partition.push_back(*it - cur_base);
      }

      uint64_t universe_bits = ceil_log2(universe);
      bvb.append_bits(cur_base, universe_bits);
      // write universe only if non-singleton and not tight
      if (n > 1) {
        if (cur_base + cur_partition.back() + 1 == universe) {
          // tight universe
          write_delta(bvb, 0);
        } else {
          write_delta(bvb, cur_partition.back());
        }
      }

      base_sequence_type::write(bvb, cur_partition.begin(),
                                cur_partition.back() + 1, cur_partition.size(),
                                params);
    } else {
      bit_vector_builder bv_sequences;
      std::vector<uint64_t> endpoints;
      std::vector<uint64_t> upper_bounds;

      uint64_t cur_i = 0;
      Iterator it = begin;
      cur_base = *begin;
      upper_bounds.push_back(cur_base);

      for (size_t p = 0; p < partitions; ++p) {
        cur_partition.clear();
        uint64_t value = 0;
        for (; cur_i < ((p + 1) * partition_size) && cur_i < n; ++cur_i, ++it) {
          value = *it;
          cur_partition.push_back(value - cur_base);
        }
        assert(cur_partition.size() <= partition_size);
        assert((p == partitions - 1) || cur_partition.size() == partition_size);

        uint64_t upper_bound = value;
        assert(cur_partition.size() > 0);
        base_sequence_type::write(bv_sequences, cur_partition.begin(),
                                  cur_partition.back() + 1,
                                  cur_partition.size(),  // XXX skip last one?
                                  params);
        endpoints.push_back(bv_sequences.size());
        upper_bounds.push_back(upper_bound);
        cur_base = upper_bound + 1;
      }

      bit_vector_builder bv_upper_bounds;
      compact_elias_fano::write(bv_upper_bounds, upper_bounds.begin(), universe,
                                partitions + 1, params);

      uint64_t endpoint_bits = ceil_log2(bv_sequences.size() + 1);
      write_gamma(bvb, endpoint_bits);
      bvb.append(bv_upper_bounds);

      for (uint64_t p = 0; p < endpoints.size() - 1; ++p) {
        bvb.append_bits(endpoints[p], endpoint_bits);
      }

      bvb.append(bv_sequences);
    }
  }

  class enumerator {
   public:
    typedef std::pair<uint64_t, uint64_t> value_type;  // (position, value)

    enumerator() {}

    enumerator(bit_vector const& bv, uint64_t offset, uint64_t universe,
               uint64_t n, global_parameters const& params)
        : m_params(params), m_size(n), m_universe(universe), m_bv(&bv) {
      bit_vector::enumerator it(bv, offset);
      m_partitions = read_gamma_nonzero(it);
      if (m_partitions == 1) {
        m_cur_partition = 0;
        m_cur_begin = 0;
        m_cur_end = n;

        uint64_t universe_bits = ceil_log2(universe);
        m_cur_base = it.take(universe_bits);
        uint64_t ub = 0;
        if (n > 1) {
          uint64_t universe_delta = read_delta(it);
          ub = universe_delta ? universe_delta : (universe - m_cur_base - 1);
        }

        m_partition_enum =
            base_sequence_enumerator(*m_bv, it.position(), ub + 1, n, m_params);

        m_cur_upper_bound = m_cur_base + ub;
      } else {
        m_endpoint_bits = read_gamma(it);
        uint64_t cur_offset = it.position();

        m_upper_bounds = compact_elias_fano::enumerator(
            bv, cur_offset, universe, m_partitions + 1, params);
        cur_offset +=
            compact_elias_fano::offsets(0, universe, m_partitions + 1, params)
                .end;

        m_endpoints_offset = cur_offset;
        uint64_t endpoints_size = m_endpoint_bits * (m_partitions - 1);
        cur_offset += endpoints_size;

        m_sequences_offset = cur_offset;
      }

      m_position = size();
      slow_move();
    }

    value_type QS_ALWAYSINLINE move(uint64_t position) {
      assert(position <= size());
      m_position = position;

      if (m_position >= m_cur_begin && m_position < m_cur_end) {
        uint64_t val =
            m_cur_base + m_partition_enum.move(m_position - m_cur_begin).second;
        return value_type(m_position, val);
      }

      return slow_move();
    }

    // note: this is instantiated oly if BaseSequence has next_geq
    value_type QS_ALWAYSINLINE next_geq(uint64_t lower_bound) {
      if (QS_LIKELY(lower_bound >= m_cur_base &&
                    lower_bound <= m_cur_upper_bound)) {
        auto val = m_partition_enum.next_geq(lower_bound - m_cur_base);
        m_position = m_cur_begin + val.first;
        return value_type(m_position, m_cur_base + val.second);
      }
      return slow_next_geq(lower_bound);
    }

    value_type QS_ALWAYSINLINE next() {
      ++m_position;

      if (QS_LIKELY(m_position < m_cur_end)) {
        uint64_t val = m_cur_base + m_partition_enum.next().second;
        return value_type(m_position, val);
      }
      return slow_next();
    }

    uint64_t size() const { return m_size; }

    uint64_t prev_value() const {
      if (QS_UNLIKELY(m_position == m_cur_begin)) {
        return m_cur_partition ? m_cur_base - 1 : 0;
      } else {
        return m_cur_base + m_partition_enum.prev_value();
      }
    }

   private:
    // the compiler does not seem smart enough to figure out that this
    // is a very unlikely condition, and inlines the move(0) inside the
    // next(), causing the code to grow. Since next is called in very
    // tight loops, on microbenchmarks this causes an improvement of
    // about 3ns on my i7 3Ghz
    value_type QS_NOINLINE slow_next() {
      if (QS_UNLIKELY(m_position == m_size)) {
        assert(m_cur_partition == m_partitions - 1);
        auto val = m_partition_enum.next();
        assert(val.first == m_partition_enum.size());
        (void)val;
        return value_type(m_position, m_universe);
      }

      switch_partition(m_cur_partition + 1);
      uint64_t val = m_cur_base + m_partition_enum.move(0).second;
      return value_type(m_position, val);
    }

    value_type QS_NOINLINE slow_move() {
      if (m_position == size()) {
        if (m_partitions > 1) {
          switch_partition(m_partitions - 1);
        }
        m_partition_enum.move(m_partition_enum.size());
        return value_type(m_position, m_universe);
      }
      uint64_t partition = m_position >> m_params.log_partition_size;
      switch_partition(partition);
      uint64_t val =
          m_cur_base + m_partition_enum.move(m_position - m_cur_begin).second;
      return value_type(m_position, val);
    }

    value_type QS_NOINLINE slow_next_geq(uint64_t lower_bound) {
      if (m_partitions == 1) {
        if (lower_bound < m_cur_base) {
          return move(0);
        } else {
          return move(size());
        }
      }

      auto ub_it = m_upper_bounds.next_geq(lower_bound);
      if (ub_it.first == 0) {
        return move(0);
      }

      if (ub_it.first == m_upper_bounds.size()) {
        return move(size());
      }

      switch_partition(ub_it.first - 1);
      return next_geq(lower_bound);
    }

    void switch_partition(uint64_t partition) {
      assert(m_partitions > 1);

      uint64_t endpoint =
          partition ? m_bv->get_bits(m_endpoints_offset +
                                         (partition - 1) * m_endpoint_bits,
                                     m_endpoint_bits)
                    : 0;
      m_bv->data().prefetch((m_sequences_offset + endpoint) / 64);

      m_cur_partition = partition;
      m_cur_begin = partition << m_params.log_partition_size;
      m_cur_end =
          std::min(size(), (partition + 1) << m_params.log_partition_size);

      auto ub_it = m_upper_bounds.move(partition + 1);
      m_cur_upper_bound = ub_it.second;
      m_cur_base = m_upper_bounds.prev_value() + (partition ? 1 : 0);

      m_partition_enum =
          base_sequence_enumerator(*m_bv, m_sequences_offset + endpoint,
                                   m_cur_upper_bound - m_cur_base + 1,
                                   m_cur_end - m_cur_begin, m_params);
    }

    global_parameters m_params;
    uint64_t m_partitions;
    uint64_t m_endpoints_offset;
    uint64_t m_endpoint_bits;
    uint64_t m_sequences_offset;
    uint64_t m_size;
    uint64_t m_universe;

    uint64_t m_position;
    uint64_t m_cur_partition;
    uint64_t m_cur_begin;
    uint64_t m_cur_end;
    uint64_t m_cur_base;
    uint64_t m_cur_upper_bound;

    bit_vector const* m_bv;
    compact_elias_fano::enumerator m_upper_bounds;
    base_sequence_enumerator m_partition_enum;
  };
};

}  // namespace ROCKSDB_NAMESPACE