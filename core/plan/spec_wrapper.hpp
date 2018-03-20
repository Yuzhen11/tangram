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
  int staleness = 0;
  int checkpoint_interval = 0;
  std::string description;
  MapJoinSpec() = default;
  MapJoinSpec(int mid, int jid, int iter, int s, int cp, std::string d)
      : map_collection_id(mid), join_collection_id(jid), 
        num_iter(iter), staleness(s), checkpoint_interval(cp),
        description(d) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << map_collection_id << join_collection_id 
        << num_iter << staleness << checkpoint_interval
        << description;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> map_collection_id >> join_collection_id
        >> num_iter >> staleness >> checkpoint_interval
        >> description;
  }
  virtual std::string DebugString() const {
    std::stringstream ss;
    ss << "map_collection_id: " << map_collection_id;
    ss << ", join_collection_id: " << join_collection_id;
    ss << ", num_iter: " << num_iter;
    ss << ", staleness: " << staleness;
    ss << ", checkpoint_interval: " << checkpoint_interval;
    ss << ", description: " << description;
    return ss.str();
  }
};

struct MapWithJoinSpec : public MapJoinSpec {
  int with_collection_id;
  MapWithJoinSpec() = default;
  MapWithJoinSpec(int mid, int jid, int iter, int s, int cp, int wid, std::string d)
      : MapJoinSpec(mid, jid, iter, s, cp, d), with_collection_id(wid) {}
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

struct CheckpointSpec : public Spec {
  int cid;
  std::string url;
  CheckpointSpec() = default;
  CheckpointSpec(int _cid, std::string _url):cid(_cid), url(_url) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << cid << url;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> cid >> url;
  }
  virtual std::string DebugString() const {
    std:: stringstream ss;
    ss << "collection_id: " << cid;
    ss << ", url: " << url;
    return ss.str();
  }
};

struct LoadCheckpointSpec : public Spec {
  int cid;
  std::string url;
  LoadCheckpointSpec() = default;
  LoadCheckpointSpec(int _cid, std::string _url):cid(_cid), url(_url) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << cid << url;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> cid >> url;
  }
  virtual std::string DebugString() const {
    std:: stringstream ss;
    ss << "collection_id: " << cid;
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
    kWrite,
    kCheckpoint,
    kLoadCheckpoint
  };
  static constexpr const char* TypeName[] = {
    "kDistribute", 
    "kLoad", 
    "kInit", 
    "kMapJoin",
    "kMapWithJoin",
    "kWrite",
    "kCheckpoint",
    "kLoadCheckpoint"
  };
  Type type;
  int id;
  std::string name;
  std::shared_ptr<Spec> spec;
  std::string DebugString() const {
    std::stringstream ss;
    ss << "type: " << TypeName[static_cast<int>(type)];
    ss << ", id: " << id;
    ss << ", name: " << name;
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
    stream << s.type << s.id << s.name;
    s.spec->ToBin(stream);
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, SpecWrapper& s) {
    stream >> s.type >> s.id >> s.name;
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
    } else if (s.type == Type::kCheckpoint){
      s.spec = std::make_shared<CheckpointSpec>();
      s.spec->FromBin(stream);
    }  else if (s.type == Type::kLoadCheckpoint){
      s.spec = std::make_shared<LoadCheckpointSpec>();
      s.spec->FromBin(stream);
    } else {
      CHECK(false);
    }
  	return stream;
  }
};


}  // namespace xyz

