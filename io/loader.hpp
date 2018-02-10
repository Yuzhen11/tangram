#pragma once

namespace xyz {

class Loader: public Actor {
 public:
  Loader(int qid, std::shared_ptr<AbstractSender> sender)
      : Actor(qid) {
    Start();
  }
  virtual ~Loader() {
    Stop();
  }
  virtual void Process(Message msg) override;
  void FetchBlock(SArrayBinStream bin);
 private:
  std::shared_ptr<AbstractSender> sender_;

  char* data_;
  hdfsFile file_ = NULL;
  hdfsFS fs_;
  size_t hdfs_block_size_;
  size_t offset_ = 0;

  std::vector<std::string> v;
};

}  // namespace xyz


