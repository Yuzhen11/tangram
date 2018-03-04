#include "io/hdfs_reader.hpp"

#include "glog/logging.h"

using namespace xyz;

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  const std::string namenode = "proj10";
  const int port = 9000;

  const std::string url = "/datasets/classification/kdd12-5blocks";
  std::vector<size_t> offsets{0, 1048576, 2097152, 3145728, 4194304};
  // block api
  int c = 0;
  for (auto offset : offsets) {
    HdfsReader reader;
    reader.Init(namenode, port, url, offset);
    auto a = reader.ReadBlock();
    c += a.size();
  }
  LOG(INFO) << c << " lines in total.";

  // iterator api
  c = 0;
  for (auto offset : offsets) {
    HdfsReader reader;
    reader.Init(namenode, port, url, offset);
    while (reader.HasLine()) {
      auto s = reader.GetLine();
    }
    c += reader.GetNumLineRead();
  }
  LOG(INFO) << c << " lines in total.";
}
