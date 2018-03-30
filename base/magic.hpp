#include <string>
#include "glog/logging.h"

namespace xyz {
namespace {

// combine timeout
// <0 : send without combine 
// 0: directly combine and send
// 0-kMaxCombineTimeout: timeout in ms
// >kMaxCombineTimeout: shuffle combine
// used in DelayedCombiner (worker/delayed_combiner).
const int kMaxCombineTimeout = 10000;

const int kShuffleCombine = kMaxCombineTimeout + 1;
const int kDirectCombine = 0;
const int kNoCombine = -1;

int ParseCombineTimeout(std::string s) {
  if (s == "kShuffleCombine") {
    return kShuffleCombine;
  } else if (s == "kDirectCombine") {
    return kDirectCombine;
  } else if (s == "kNoCombine") {
    return kNoCombine;
  } else {
    int timeout;
    try {
      timeout = std::stoi(s);
      if (timeout > kMaxCombineTimeout || timeout <= kDirectCombine) {
        CHECK(false) << "invalid combine_timeout: " << s;
      }
    } catch (...) {
      CHECK(false) << "invalid combine_timeout: " << s;
    }
    return timeout;
  }
}

}  // namespace
}  // namespace xyz

