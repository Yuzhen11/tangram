#include "io/assigner.hpp"
#include "io/hdfs_browser.hpp"

#include "comm/simple_sender.hpp"

using namespace xyz;

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);

  const int qid = 0;
  const std::string namenode = "proj10";
  const int port = 9000;
  std::string url = "/datasets/classification/a9";
  auto sender = std::make_shared<SimpleSender>();
  auto browser = std::make_shared<HDFSBrowser>(namenode, port);

  Assigner assigner(0, sender, browser);
  int blocks = assigner.Load(url, {{"proj5", 0}}, 1);
  LOG(INFO) << "blocks number: " << blocks;
  auto* q = assigner.GetWorkQueue();

  SArrayBinStream ctrl_bin, bin;
  ctrl_bin << int(0);
  std::pair<std::string, int> n1{"node5", 0};
  bin << n1;
  Message msg;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  q->Push(msg);
}

