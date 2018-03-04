#include "io/hdfs_writer.hpp"

#include <string>

#include "glog/logging.h"

using namespace xyz;

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  const std::string namenode = "proj10";
  const int port = 9000;
  HdfsWriter writer(namenode, port);
  std::string content = "hello world";
  const std::string dest_url = "/tmp/tmp/a.txt";
  bool rc = writer.Write(dest_url, content.c_str(), content.size());
  CHECK_EQ(rc, 0);
}
