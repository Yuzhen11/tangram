#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/sarray_binstream.hpp"

namespace xyz {
namespace {

class TestSArrayBinStream : public testing::Test {};

TEST_F(TestSArrayBinStream, ConstructAndSize) {
  SArrayBinStream bin;
  ASSERT_EQ(bin.Size(), 0);
}

TEST_F(TestSArrayBinStream, FromSArrayToSArray) {
  SArrayBinStream bin;
  third_party::SArray<int> s{1, 2};
  bin.FromSArray(s);
  ASSERT_EQ(bin.Size(), 8);
  int a, b;
  bin >> a >> b;
  ASSERT_EQ(bin.Size(), 0);
  EXPECT_EQ(a, 1);
  EXPECT_EQ(b, 2);

  auto new_s = bin.ToSArray();
  auto new_ss = static_cast<third_party::SArray<int>>(new_s);
  ASSERT_EQ(new_ss.size(), 2);
  EXPECT_EQ(new_ss[0], 1);
  EXPECT_EQ(new_ss[1], 2);
}

TEST_F(TestSArrayBinStream, FromMsg) {
  SArrayBinStream bin;
  Message msg;
  third_party::SArray<int> s{1, 2};
  msg.AddData(s);
  bin.FromMsg(msg);
  ASSERT_EQ(bin.Size(), 8);
  int a, b;
  bin >> a >> b;
  ASSERT_EQ(bin.Size(), 0);
  EXPECT_EQ(a, 1);
  EXPECT_EQ(b, 2);
}

TEST_F(TestSArrayBinStream, ToMsg) {
  SArrayBinStream bin;
  int a = 1;
  int b = 2;
  bin << a << b;
  Message msg = bin.ToMsg();
  ASSERT_EQ(msg.data.size(), 1);
  third_party::SArray<int> s = third_party::SArray<int>(msg.data[0]);
  ASSERT_EQ(s.size(), 2);
  EXPECT_EQ(s[0], a);
  EXPECT_EQ(s[1], b);
}

TEST_F(TestSArrayBinStream, Int) {
  SArrayBinStream bin;
  int a = 10;
  int b = 20;
  bin << a << b;
  ASSERT_EQ(bin.Size(), 8);
  int c, d;
  bin >> c >> d;
  EXPECT_EQ(c, a);
  EXPECT_EQ(d, b);
  ASSERT_EQ(bin.Size(), 0);
}

TEST_F(TestSArrayBinStream, String) {
  SArrayBinStream bin;
  std::string a = "hi";
  std::string b = "hello";
  bin << a << b;
  std::string c, d;
  bin >> c >> d;
  EXPECT_EQ(c, a);
  EXPECT_EQ(d, b);
  ASSERT_EQ(bin.Size(), 0);
}

TEST_F(TestSArrayBinStream, Vector) {
  SArrayBinStream bin;
  std::vector<int> v{3, 5, 8};
  bin << v;
  std::vector<int> a;
  bin >> a;
  EXPECT_EQ(a, v);
}

TEST_F(TestSArrayBinStream, SArrayBinStream) {
  SArrayBinStream bin;
  SArrayBinStream bin_send;
  SArrayBinStream bin_recv;
  int a = 10;
  bin_send << a;
  bin << a << bin_send << a;

  int b, c, d;
  bin >> b >> bin_recv >> d;
  bin_recv >> c;
  EXPECT_EQ(a, b);
  EXPECT_EQ(a, c);
  EXPECT_EQ(a, d);
}

}  // namespace
}  // namespace xyz
