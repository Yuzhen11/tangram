#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/third_party/network_utils.h"

namespace xyz {
namespace third_party {
namespace {

class TestNetworkUtils : public testing::Test {};

TEST_F(TestNetworkUtils, GetAvailableInterfaceAndIP) {
  std::string interface;
  std::string ip;
  GetAvailableInterfaceAndIP(&interface, &ip);
  EXPECT_NE(interface, "");
  EXPECT_NE(ip, "");
  VLOG(1) << "interface: " << interface;
  VLOG(1) << "ip: " << ip;
}

TEST_F(TestNetworkUtils, GetAvailablePort) {
  int port = 0;
  port = GetAvailablePort();
  EXPECT_NE(port, 0);
  VLOG(1) << "port: " << port;
}

}  // namespace
}  // namespace third_party
}  // namespace xyz
