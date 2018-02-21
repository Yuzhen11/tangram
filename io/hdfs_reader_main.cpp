#include "io/hdfs_reader.hpp"

#include "glog/logging.h"

using namespace xyz;

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);

  HdfsReader reader;
  const std::string namenode = "proj10";
  const int port = 9000;
  /*
  const std::string url = "/datasets/classification/kdd12";
  const size_t offset = 18446744073692774400;
  auto a = reader.Read(namenode, port, url, offset);
  */

  const std::string url = "/datasets/classification/kdd12-5blocks";
  std::vector<size_t> offsets{0, 1048576, 2097152, 3145728, 4194304};
  int c = 0;
  for (auto offset : offsets) {
    auto a = reader.Read(namenode, port, url, offset);
    c += a.size();
  }
  LOG(INFO) << c << " lines in total.";
}
