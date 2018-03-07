#include "io/abstract_reader.hpp"

namespace xyz {

struct FakeReader : public AbstractReader {
  virtual int Read(void *buffer, size_t len) override {
    return 0;
  }

  virtual void Init(std::string url) override {
  	LOG(INFO) << "Reading from: " << url;
  }

  virtual size_t GetFileSize() override {
  	return 0;
  }
};

} // namespace xyz