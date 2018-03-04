#include "io/abstract_writer.hpp"

namespace xyz {

struct FakeWriter : public AbstractWriter {
  virtual int Write(std::string dest_url, const void *buffer, size_t len) override {
    VLOG(1) << "writing: " << len << " bytes";
    return 0;
  }
};

} // namespace xyz

