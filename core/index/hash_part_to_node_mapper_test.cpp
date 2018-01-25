#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/index/hash_part_to_node_mapper.hpp"

namespace xyz {
namespace {

class TestHashPartToNodeMapper : public testing::Test {};

TEST_F(TestHashPartToNodeMapper, Construct) {
  HashPartToNodeMapper mapper(4);
}

TEST_F(TestHashPartToNodeMapper, Get) {
  HashPartToNodeMapper mapper(4);
  VLOG(1) << mapper.Get(2);
}

}  // namespace
}  // namespace xyz

