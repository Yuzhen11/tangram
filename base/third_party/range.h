/**
 * From ps-lite
 */
#pragma once

#include <cstdint>

namespace xyz {
namespace third_party {

/**
 * \brief a range [begin, end)
 */
class Range {
 public:
  Range() : Range(0, 0) {}
  Range(uint64_t begin, uint64_t end) : begin_(begin), end_(end) { }

  uint64_t begin() const { return begin_; }
  uint64_t end() const { return end_; }
  uint64_t size() const { return end_ - begin_; }
/* 
  friend SArrayBinStream& operator<<(SArrayBinStream& stream, const third_party::Range& range) {
    stream << range.begin() << range.end();
    return stream;
  }
  friend SArrayBinStream& operator>>(SArrayBinStream& stream, third_party::Range& range) {
    stream >> range.begin() >> range.end();
    return stream;
  }
 */
 private:
  uint64_t begin_;
  uint64_t end_;
};
 
}  // namespace third_party
}  // namespace xyz
