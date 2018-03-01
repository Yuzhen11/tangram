#include "io/assigner.hpp"
#include "io/hdfs_browser.hpp"
#include "io/meta.hpp"

#include "comm/simple_sender.hpp"

using namespace xyz;

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);

  const int qid = 0;
  const std::string namenode = "proj10";
  const int port = 9000;
  std::string url = "/datasets/classification/kdd12-5blocks";
  auto sender = std::make_shared<SimpleSender>();
  auto browser = std::make_shared<HDFSBrowser>(namenode, port);

  Assigner assigner(sender, browser);
  int collection_id = 0;
  int num_blocks = assigner.Load(collection_id, url, {{"proj5", 0}}, 1);
  LOG(INFO) << "blocks number: " << num_blocks;

  for (int i = 0; i < num_blocks; ++ i) {
    // recv
    auto recv_msg = sender->Get();
    SArrayBinStream recv_bin;
    CHECK_EQ(recv_msg.data.size(), 2);
    recv_bin.FromSArray(recv_msg.data[1]);
    AssignedBlock block;
    recv_bin >> block;
    LOG(INFO) << "block: " << block.DebugString();

    // send finish
    FinishedBlock b{block.id, 0, 0, "node5"};
    CHECK_EQ(assigner.Done(), false);
    assigner.FinishBlock(b);
  }
  CHECK_EQ(assigner.Done(), true);
  LOG(INFO) << assigner.DebugStringFinishedBlocks();
}

