#pragma once

#include <sstream>

namespace xyz {

class SArrayBinStream;
struct Node {
  static const int kEmpty;

  //Role role;
  int id;
  std::string hostname;
  int port;
  bool is_recovery;
  

  std::string DebugString() const {
    std::stringstream ss;
    ss << " { id=" << id << " hostname=" << hostname << " port=" << port << " is_recovery=" << is_recovery << " }";
    return ss.str();
  }

  bool operator==(const Node& other) const {
    return id == other.id && hostname == other.hostname && port == other.port && is_recovery == other.is_recovery;
  }

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Node& node);
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Node& node);
};

}  // namespace xyz
