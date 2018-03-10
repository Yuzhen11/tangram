#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

namespace xyz {

struct Spec {
  virtual ~Spec() = default;
  virtual void ToBin(SArrayBinStream& bin) = 0;
  virtual void FromBin(SArrayBinStream& bin) = 0;
  virtual std::string DebugString() const = 0;
};

struct MapJoinSpec : public Spec {
  int map_collection_id;
  int join_collection_id;
  int num_iter = 1;
  MapJoinSpec() = default;
  MapJoinSpec(int mid, int jid, int iter)
      : map_collection_id(mid), join_collection_id(jid), num_iter(iter) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << map_collection_id << join_collection_id << num_iter;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> map_collection_id >> join_collection_id >> num_iter;
  }
  virtual std::string DebugString() const {
    std::stringstream ss;
    ss << "map_collection_id: " << map_collection_id;
    ss << ", join_collection_id: " << join_collection_id;
    ss << ", num_iter: " << num_iter;
    return ss.str();
  }
};

struct MapWithJoinSpec : public MapJoinSpec {
  int with_collection_id;
  MapWithJoinSpec() = default;
  MapWithJoinSpec(int mid, int jid, int iter, int wid)
      : MapJoinSpec(mid, jid, iter), with_collection_id(wid) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    MapJoinSpec::ToBin(bin);
    bin << with_collection_id;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    MapJoinSpec::FromBin(bin);
    bin >> with_collection_id;
  }
  virtual std::string DebugString() const {
    std::stringstream ss;
    ss << MapJoinSpec::DebugString();
    ss << ", with_collection_id: " << with_collection_id;
    return ss.str();
  }
};

struct WriteSpec : public Spec {
  int collection_id;
  std::string url;
  WriteSpec() = default;
  WriteSpec(int cid, std::string _url):collection_id(cid), url(_url) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << collection_id << url;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> collection_id >> url;
  }
  virtual std::string DebugString() const {
    std:: stringstream ss;
    ss << "collection_id: " << collection_id;
    ss << ", url: " << url;
    return ss.str();
  }
};

struct LoadSpec : public Spec {
  int collection_id;
  std::string url;
  LoadSpec() = default;
  LoadSpec(int cid, std::string _url):collection_id(cid), url(_url) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << collection_id << url;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> collection_id >> url;
  }
  virtual std::string DebugString() const {
    std:: stringstream ss;
    ss << "collection_id: " << collection_id;
    ss << ", url: " << url;
    return ss.str();
  }
};

struct DistributeSpec: public Spec {
  int collection_id;
  int num_partition;
  SArrayBinStream data;
  DistributeSpec() = default;
  DistributeSpec(int c_id, int num_parts, SArrayBinStream _data)
      :collection_id(c_id), num_partition(num_parts), data(_data) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << collection_id << num_partition << data;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> collection_id >> num_partition >> data;
  }
  virtual std::string DebugString() const {
    std::stringstream ss;
    ss << "collection_id: " << collection_id;
    ss << ", num_partition: " << num_partition;
    ss << ", data size in bytes: " << data.Size();
    return ss.str();
  }
};

struct SpecWrapper {
  enum class Type : char {
    kDistribute, 
    kLoad, 
    kInit, 
    kMapJoin, 
    kMapWithJoin, 
    kWrite
  };
  static constexpr const char* TypeName[] = {
    "kDistribute", 
    "kLoad", 
    "kInit", 
    "kMapJoin",
    "kMapWithJoin",
    "kWrite"
  };
  Type type;
  int id;
  std::shared_ptr<Spec> spec;
  std::string DebugString() const {
    std::stringstream ss;
    ss << "type: " << TypeName[static_cast<int>(type)];
    ss << ", id: " << id;
    ss << ", spec: " << spec->DebugString();
    return ss.str();
  }

  template<typename SpecType, typename... Args>
  void SetSpec(int _id, Type _type, Args... args) {
    id = _id;
    type = _type;
    spec = std::make_shared<SpecType>(args...);
  }


  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const SpecWrapper& s) {
    stream << s.type << s.id;
    s.spec->ToBin(stream);
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, SpecWrapper& s) {
    stream >> s.type >> s.id;
    if (s.type == Type::kDistribute) {
      s.spec = std::make_shared<DistributeSpec>();
      s.spec->FromBin(stream);
    } else if (s.type == Type::kMapJoin){
      s.spec = std::make_shared<MapJoinSpec>();
      s.spec->FromBin(stream);
    } else if (s.type == Type::kMapWithJoin){
      s.spec = std::make_shared<MapWithJoinSpec>();
      s.spec->FromBin(stream);
    } else if (s.type == Type::kLoad){
      s.spec = std::make_shared<LoadSpec>();
      s.spec->FromBin(stream);
    } else if (s.type == Type::kWrite){
      s.spec = std::make_shared<WriteSpec>();
      s.spec->FromBin(stream);
    } else {
      CHECK(false);
    }
  	return stream;
  }
};


}  // namespace xyz
