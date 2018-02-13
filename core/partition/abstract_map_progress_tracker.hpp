#pragma once

namespace xyz {

struct AbstractMapProgressTracker {
  virtual ~AbstractMapProgressTracker() {}

  // The map will report progress through this interface
  virtual void Report(int) = 0;
};

}  // namespace xyz

