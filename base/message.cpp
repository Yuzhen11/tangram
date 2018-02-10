#include "base/message.hpp"
#include "base/sarray_binstream.hpp"

namespace xyz {
    SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Control& ctrl) {
        stream << static_cast<char>(ctrl.flag) << ctrl.partition_id << ctrl.collection_id << ctrl.node << ctrl.is_recovery << ctrl.timestamp;
        return stream;
    }

    SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Control& ctrl) {
        char ch;
        stream >> ch;
        ctrl.flag = static_cast<Flag>(ch);
        stream >> ctrl.partition_id >> ctrl.collection_id >> ctrl.node >> ctrl.is_recovery >> ctrl.timestamp;
        return stream;
    }
}  // namespace xyz
