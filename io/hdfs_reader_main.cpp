#include "io/hdfs_reader.hpp"

#include <string>

#include "glog/logging.h"

using namespace xyz;

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  const std::string namenode = "proj10";
  const int port = 9000;
  HdfsReader reader(namenode, port);
  const std::string url = "/tmp/read/a.txt";
  reader.Init(url);
  size_t len = reader.GetFileSize();
  char * data = new char[len];
  int rc = reader.Read(data, len);
  CHECK_EQ(rc, 0);
  LOG(INFO) << "File content: " << data;

  delete [] data;
}
