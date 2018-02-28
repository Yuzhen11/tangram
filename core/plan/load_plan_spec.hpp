#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

namespace xyz {

struct LoadPlanSpec {
  int load_collection_id;
  int load_partition_num;
  std::string url;
  std::string namenode;
  int port;

  LoadPlanSpec() = default;
  LoadPlanSpec(int mid, std::string u, std::string hdfs_namenode, int namenode_port)
      : load_collection_id(mid), url(u), namenode(hdfs_namenode), port(namenode_port)
  {}

  std::string DebugString() const {
    std::stringstream ss;
    ss << ", load_collection_id: " << load_collection_id;
    ss << ", load_partition_num: "<< load_partition_num;
    ss << ", url: " << url;
    ss << ", namenode: " << namenode;
    ss << ", port: " << port;
    ss << "}";
    return ss.str();
}

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const LoadPlanSpec& p) {
    stream << p.load_collection_id << p.load_partition_num << p.url << p.namenode << p.port;
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, LoadPlanSpec& p) {
    stream >> p.load_collection_id >> p.load_partition_num >> p.url >> p.namenode >> p.port;
  	return stream;
  }
};

}  // namespace xyz
