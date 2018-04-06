#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

namespace xyz {

using ReadWriteVector = std::pair<std::vector<int>, std::vector<int>>;

struct Spec {
  virtual ~Spec() = default;
  virtual void ToBin(SArrayBinStream& bin) = 0;
  virtual void FromBin(SArrayBinStream& bin) = 0;
  virtual std::string DebugString() const = 0;
  virtual ReadWriteVector GetReadWrite() const = 0;
};

struct MapJoinSpec : public Spec {
  int map_collection_id;
  int join_collection_id;
  int combine_timeout = 0;
  int num_iter = 1;
  int staleness = 0;
  int checkpoint_interval = 0;
  std::string checkpoint_path;
  std::string description;
  MapJoinSpec() = default;
  MapJoinSpec(int mid, int jid, int comb, int iter, int s, 
          int cp, std::string path, std::string d)
      : map_collection_id(mid), join_collection_id(jid), 
        combine_timeout(comb), num_iter(iter), staleness(s), checkpoint_interval(cp),
        checkpoint_path(path), description(d) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << map_collection_id << join_collection_id 
        << combine_timeout << num_iter << staleness << checkpoint_interval
        << checkpoint_path << description;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> map_collection_id >> join_collection_id
        >> combine_timeout >> num_iter >> staleness >> checkpoint_interval
        >> checkpoint_path >> description;
  }
  virtual ReadWriteVector GetReadWrite() const {
    if (map_collection_id == join_collection_id) {
      return {{}, {map_collection_id}};
    } else {
      return {{map_collection_id}, {join_collection_id}};
    }
  }
  virtual std::string DebugString() const {
    std::stringstream ss;
    ss << "map_collection_id: " << map_collection_id;
    ss << ", join_collection_id: " << join_collection_id;
    ss << ", combine_timeout: " << combine_timeout;
    ss << ", num_iter: " << num_iter;
    ss << ", staleness: " << staleness;
    ss << ", checkpoint_interval: " << checkpoint_interval;
    ss << ", checkpoint_path: " << checkpoint_path;
    ss << ", description: " << description;
    return ss.str();
  }
};

struct MapWithJoinSpec : public MapJoinSpec {
  int with_collection_id;
  MapWithJoinSpec() = default;
  MapWithJoinSpec(int mid, int jid, int comb, int iter, int s, int cp, 
          std::string path, std::string d, int wid)
      : MapJoinSpec(mid, jid, comb, iter, s, cp, path, d), with_collection_id(wid) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    MapJoinSpec::ToBin(bin);
    bin << with_collection_id;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    MapJoinSpec::FromBin(bin);
    bin >> with_collection_id;
  }
  virtual ReadWriteVector GetReadWrite() const {
    std::vector<int> read;
    if (map_collection_id != join_collection_id) {
      read.push_back(map_collection_id);
    }
    if (with_collection_id != join_collection_id) {
      read.push_back(with_collection_id);
    }
    return {read, {join_collection_id}};
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
  virtual ReadWriteVector GetReadWrite() const {
    return {{}, {collection_id}};
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
  bool is_load_meta;
  bool is_whole_file;
  LoadSpec() = default;
  LoadSpec(int cid, std::string _url, bool _is_load_meta, bool _is_whole_file)
    :collection_id(cid), url(_url), is_load_meta(_is_load_meta), is_whole_file(_is_whole_file) {}
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << collection_id << url << is_load_meta << is_whole_file;
  }
  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> collection_id >> url >> is_load_meta >> is_whole_file;
  }
  virtual ReadWriteVector GetReadWrite() const {
    return {{}, {collection_id}};
  }
  virtual std::string DebugString() const {
    std:: stringstream ss;
    ss << "collection_id: " << collection_id;
    ss << ", url: " << url;
    ss << ", is_load_meta: " << is_load_meta;
    ss << ", is_whole_file: " << is_whole_file;
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
  virtual ReadWriteVector GetReadWrite() const {
    return {{cid}, {}};
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
  virtual ReadWriteVector GetReadWrite() const {
    return {{}, {cid}};
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
  virtual ReadWriteVector GetReadWrite() const {
    return {{}, {collection_id}};
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

  MapJoinSpec* GetMapJoinSpec();
  MapWithJoinSpec* GetMapWithJoinSpec();
  DistributeSpec* GetDistributeSpec();
  LoadSpec* GetLoadSpec();
  CheckpointSpec* GetCheckpointSpec();
  LoadCheckpointSpec* GetLoadCheckpointSpec();
  WriteSpec* GetWriteSpec();

  ReadWriteVector GetReadWrite() {
    return spec->GetReadWrite();
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

