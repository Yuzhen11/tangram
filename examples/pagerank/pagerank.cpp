#include "boost/tokenizer.hpp"
#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

// #define ENABLE_CP

DEFINE_int32(num_parts, 100, "# num of partitions");
DEFINE_string(url, "", "The url for hdfs file");
DEFINE_string(combine_type, "kDirectCombine",
              "kShuffleCombine, kDirectCombine, kNoCombine, timeout");

using namespace xyz;

struct Vertex {
  using KeyT = int;

  Vertex() : pr(0.15){};
  Vertex(KeyT vertex) : vertex(vertex), pr(0.15) {}
  KeyT Key() const { return vertex; }

  KeyT vertex;
  std::vector<int> outlinks;
  float pr;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Vertex &vertex) {
    stream << vertex.vertex << vertex.outlinks << vertex.pr;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Vertex &vertex) {
    stream >> vertex.vertex >> vertex.outlinks >> vertex.pr;
    return stream;
  }
};

int main(int argc, char **argv) {
  Runner::Init(argc, argv);
  const int combine_timeout = ParseCombineTimeout(FLAGS_combine_type);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "combine_type: " << FLAGS_combine_type
              << ", timeout: " << combine_timeout;
  }

  auto loaded_dataset =
      Context::load(FLAGS_url, [](std::string s) {
        Vertex v;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(s, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it =
            tok.begin();

        v.vertex = std::stoi(*it++);
        it++;
        while (it != tok.end()) {
          v.outlinks.push_back(std::stoi(*it++));
        }

        return v;
      })->SetName("dataset");

  auto vertex =
      Context::placeholder<Vertex>(FLAGS_num_parts)->SetName("vertex");

  Context::mappartupdate(
      loaded_dataset, vertex,
      [](TypedPartition<Vertex> *p, Output<int, std::vector<int>> *o) {
        for (auto &v : *p) {
          o->Add(v.vertex, v.outlinks);
          for (auto outlink : v.outlinks) {
            o->Add(outlink, std::vector<int>());
          }
        }
      },
      [](Vertex *v, std::vector<int> outlinks) {
        for (auto outlink : outlinks) {
          v->outlinks.push_back(outlink);
        }
        v->pr = 0.15;
      })
      ->SetCombine([](std::vector<int> *msg1, std::vector<int> msg2) {
        for (int value : msg2)
          msg1->push_back(value);
      })
      ->SetName("construct vertex");

  Context::sort_each_partition(vertex);

#ifdef ENABLE_CP
  Context::checkpoint(vertex, "/tmp/tmp/yz");
#endif

  auto p2 =
      Context::mappartupdate(
          vertex, vertex,
          [](TypedPartition<Vertex> *p, Output<int, float> *o) {
            for (auto &v : *p) {
              for (auto outlink : v.outlinks) {
                o->Add(outlink, v.pr / v.outlinks.size());
              }
            }
          },
          [](Vertex *v, float contrib) { v->pr += 0.85 * contrib; })
          ->SetCombine([](float *a, float b) { *a = *a + b; }, combine_timeout)
          ->SetIter(5)
          ->SetStaleness(0)
#ifdef ENABLE_CP
          ->SetCheckpointInterval(5, "/tmp/tmp/yz")
#endif
          ->SetName("pagerank main logic");

  // Context::count(vertex);
  Runner::Run();
}
