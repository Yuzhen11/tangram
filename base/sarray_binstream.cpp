#include "base/sarray_binstream.hpp"

namespace xyz {

size_t SArrayBinStream::Size() const { return buffer_.size() - front_; }

void SArrayBinStream::AddBin(const char* bin, size_t sz) {
  buffer_.append_bytes(bin, sz);
}

void* SArrayBinStream::PopBin(size_t sz) {
  CHECK_LE(front_ + sz, buffer_.size());
  void* ret = &buffer_[front_];
  front_ += sz;
  return ret;
}

Message SArrayBinStream::ToMsg() const {
  Message msg;
  msg.AddData(buffer_);
  return msg;
}

void SArrayBinStream::FromMsg(const Message& msg) {
  //CHECK_EQ(msg.data.size(), 1);
  FromSArray(msg.data[0]);
}

third_party::SArray<char> SArrayBinStream::ToSArray() {
  return buffer_;
}

SArrayBinStream& operator<<(SArrayBinStream& stream, const SArrayBinStream& bin) {
  stream << bin.Size();
  stream.AddBin(bin.GetPtr(), bin.Size());
  return stream;
}
SArrayBinStream& operator>>(SArrayBinStream& stream, SArrayBinStream& bin) {
  size_t len;
  stream >> len;
  CHECK(bin.Size() == 0);
  bin.CopyFrom(stream.GetPtr(), len);
  stream.PopBin(len);
  return stream;
}

}  // namespace xyz
