
namespace xyz {
namespace {

// combine timeout
// <0 : send without combine 
// 0: directly combine and send
// 0-kMaxCombineTimeout: timeout in ms
// >kMaxCombineTimeout: shuffle combine
// used in DelayedCombiner (worker/delayed_combiner).
const int kMaxCombineTimeout = 10000;

}  // namespace
}  // namespace xyz

