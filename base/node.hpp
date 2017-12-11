#pragma once

#include <sstream>

namespace xyz {

struct Node {
  static const int kEmpty;
  int id;
  std::string hostname;
  int port;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Node: { id=" << id << " hostname=" << hostname << " port=" << port << " }";
    return ss.str();
  }
  bool operator==(const Node& other) const {
    return id == other.id && hostname == other.hostname && port == other.port;
  }
};

}  // namespace xyz
