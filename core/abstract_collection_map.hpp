#pragma once

namespace xyz {

struct AbstractCollectionMap {
  virtual ~AbstractCollectionMap() {}
  virtual int Lookup(int collection_id, int part_id) = 0;
};

}  // namespace xyz

