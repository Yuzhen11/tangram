#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/hash_key_to_part_mapper.hpp"

namespace xyz {
namespace {

class TestHashKeyToPartMapper : public testing::Test {};

TEST_F(TestHashKeyToPartMapper, Construct) {
  HashKeyToPartMapper<int> m1(4);
  HashKeyToPartMapper<std::string> m2(4);
}

TEST_F(TestHashKeyToPartMapper, Get) {
  HashKeyToPartMapper<std::string> m(4);
  auto a = m.Get("Hello");
  VLOG(1) << a;
  EXPECT_EQ(a, 1);
  EXPECT_EQ(m.GetNumPart(), 4);
}

}  // namespace
}  // namespace xyz

