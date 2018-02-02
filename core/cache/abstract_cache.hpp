#pragma once

namespace xyz {

class AbstractCache {
 public:
  virtual ~AbstractCache() {}
};

template <typename ObjT>
class TypedCache : public AbstractCache {
 public:
  virtual ObjT Get(ObjT::KeyT key) = 0;
};

}  // namespace xyz

