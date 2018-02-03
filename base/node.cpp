#include "base/node.hpp"
#include "base/sarray_binstream.hpp"

#include <limits>

namespace xyz {

const int Node::kEmpty = std::numeric_limits<int>::max();


SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Node& node) {
	stream << node.id << node.hostname << node.port << node.is_recovery;
	return stream;
}

SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Node& node) {
	stream >> node.id >> node.hostname >> node.port >> node.is_recovery;
	return stream;
}

}  // namespace xyz
