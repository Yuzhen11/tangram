#pragma once

namespace xyz {

class AbstractFetcher {
 public:
  virtual ~AbstractFetcher() = default;

  virtual void FetchRemote(int collection_id, int partition_id, int version) = 0;
};

}  // namespace xyz

