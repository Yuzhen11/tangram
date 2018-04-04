#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "boost/tokenizer.hpp"

// #define ENABLE_CP

DEFINE_int32(num_parts, 100, "# num of partitions");
DEFINE_string(url, "", "The url for hdfs file");
DEFINE_string(combine_type, "kDirectCombine", "kShuffleCombine, kDirectCombine, kNoCombine, timeout");

using namespace xyz;

struct Vertex
{
  using KeyT = int;

  Vertex() : pr(0.15) {};
  Vertex(KeyT vertex) : vertex(vertex), pr(0.15) {}
  KeyT Key() const { return vertex; }

  KeyT vertex;
  std::vector<int> outlinks;
  float pr;

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Vertex& vertex) {
    stream << vertex.vertex << vertex.outlinks << vertex.pr;
    return stream; 
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Vertex& vertex) {
    stream >> vertex.vertex >> vertex.outlinks >> vertex.pr;
    return stream; 
  }
};


int main(int argc, char** argv) {
  Runner::Init(argc, argv);
  const int combine_timeout = ParseCombineTimeout(FLAGS_combine_type);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "combine_type: " << FLAGS_combine_type << ", timeout: " << combine_timeout;
  }

  auto loaded_dataset = Context::load(FLAGS_url, [](std::string& s) {
    Vertex v;
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();

    v.vertex = std::stoi(*it++);
    it++;
    while (it != tok.end()) {
      v.outlinks.push_back(std::stoi(*it++));
    }

    return v;
  })->SetName("dataset");

  auto vertex = Context::placeholder<Vertex>(FLAGS_num_parts)->SetName("vertex");

  auto p1 = Context::mapjoin(loaded_dataset, vertex,
    [](const Vertex& v) {
      return std::pair<int, std::vector<int>> (v.vertex, v.outlinks);
    },
    [](Vertex* v, std::vector<int> outlinks) {
      for (auto outlink : outlinks) {
        v->outlinks.push_back(outlink);
      }
      v->pr = 0.15;
    })->SetName("construct vertex");

  Context::sort_each_partition(vertex);

#ifdef ENABLE_CP
  Context::checkpoint(vertex, "/tmp/tmp/yz");
#endif

  auto p2 = Context::mappartjoin(vertex, vertex,
    [](TypedPartition<Vertex>* p,
      AbstractMapProgressTracker* t) {
      std::vector<std::pair<int, float>> contribs;
      for (auto& v: *p) {
        for (auto outlink : v.outlinks) {
          contribs.push_back(std::pair<int, float>(outlink, v.pr/v.outlinks.size()));
        }
      }
      return contribs;
    },
    [](Vertex* v, float contrib) {
      v->pr += 0.85 * contrib;
    })
    ->SetCombine([](float* a, float b) {
      *a = *a + b;
    }, combine_timeout)
    ->SetIter(25)
    ->SetStaleness(0)
#ifdef ENABLE_CP
    ->SetCheckpointInterval(5, "/tmp/tmp/yz")
#endif
    ->SetName("pagerank main logic");

  // Context::count(loaded_dataset);
  Runner::Run();
}
