#include "boost/tokenizer.hpp"
#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_int32(num_parts, 100, "# num of partitions");
DEFINE_string(url, "", "The url for hdfs file");
DEFINE_string(combine_type, "kDirectCombine",
              "kShuffleCombine, kDirectCombine, kNoCombine, timeout");
DEFINE_string(pr_url, "/tmp/tmp/yz/tmp/pr", "");
DEFINE_string(topk_url, "/tmp/tmp/yz/tmp/topk", "");

DEFINE_int32(num_vertices, 1, "# num of vertex");
DEFINE_int32(num_iters, 10, "# num of iters");
DEFINE_int32(staleness, 0, "");

using namespace xyz;

struct Vertex {
  using KeyT = int;

  Vertex() : pr(0){};
  Vertex(KeyT vertex) : vertex(vertex), pr(0) {}
  KeyT Key() const { return vertex; }

  KeyT vertex;
  std::vector<int> outlinks;
  float pr = 0.;
  float delta = 0.;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Vertex &vertex) {
    stream << vertex.vertex << vertex.outlinks << vertex.pr << vertex.delta;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Vertex &vertex) {
    stream >> vertex.vertex >> vertex.outlinks >> vertex.pr >> vertex.delta;
    return stream;
  }
};

struct TopK {
  using KeyT = int;
  TopK() = default;
  TopK(KeyT id) : id(id) {}
  KeyT Key() const { return id; }
  KeyT id;
  std::vector<std::pair<int, float>> vertices;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const TopK &topk) {
    stream << topk.id;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream, TopK &topk) {
    stream >> topk.id;
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

  Context::mappartjoin(
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
        // v->delta = 0.15/FLAGS_num_vertices;
        v->delta = 0.15;
        v->pr = 0;
      })
      ->SetCombine([](std::vector<int> *msg1, std::vector<int> msg2) {
        for (int value : msg2)
          msg1->push_back(value);
      })
      ->SetName("construct vertex");

  Context::sort_each_partition(vertex);

  // Context::count(vertex);

  auto p2 =
      Context::mappartjoin(
          vertex, vertex,
          [](TypedPartition<Vertex> *p, Output<int, float> *o) {
            for (auto &v : *p) {
              v.pr += v.delta;
              if (v.delta == 0) {
                continue;
              }
              for (auto outlink : v.outlinks) {
                o->Add(outlink, v.delta / v.outlinks.size());
              }
              v.delta = 0;
            }
          },
          [](Vertex *v, float contrib) { v->delta += 0.85 * contrib; })
          ->SetCombine([](float *a, float b) { *a = *a + b; }, combine_timeout)
          ->SetIter(FLAGS_num_iters)
          ->SetStaleness(FLAGS_staleness)
          ->SetName("pagerank main logic");

  Context::write(vertex, FLAGS_pr_url,
                 [](const Vertex &v, std::stringstream &ss) {
                   ss << v.vertex << " " << v.pr << "\n";
                 });

  auto topk = Context::placeholder<TopK>(1)->SetName("topk");
  Context::mapjoin(
      vertex, topk,
      [](const Vertex &vertex,
         Output<int, std::vector<std::pair<int, float>>> *o) {
        std::vector<std::pair<int, float>> vertices;
        vertices.push_back({vertex.vertex, vertex.pr});
        o->Add(0, std::move(vertices));
      },
      [](TopK *topk, std::vector<std::pair<int, float>> vertices) {
        std::vector<std::pair<int, float>> v;
        int k1 = 0;
        int k2 = 0;
        for (int i = 0; i < 10; i++) { // top 10
          if (k1 != topk->vertices.size() &&
              (k2 == vertices.size() ||
               topk->vertices.at(k1).second > vertices.at(k2).second)) {
            v.push_back(topk->vertices.at(k1++));
          } else if (k2 != vertices.size() &&
                     (k1 == topk->vertices.size() ||
                      topk->vertices.at(k1).second <= vertices.at(k2).second)) {
            v.push_back(vertices.at(k2++));
          } else {
            break;
          }
        }
        topk->vertices = v;
      })
      ->SetCombine([](std::vector<std::pair<int, float>> *v1,
                      const std::vector<std::pair<int, float>> &v2) {
        std::vector<std::pair<int, float>> v;
        int k1 = 0;
        int k2 = 0;
        for (int i = 0; i < 10; i++) { // top 10
          if (k1 != v1->size() &&
              (k2 == v2.size() || v1->at(k1).second > v2.at(k2).second)) {
            v.push_back(v1->at(k1++));
          } else if (k2 != v2.size() &&
                     (k1 == v1->size() ||
                      v1->at(k1).second <= v2.at(k2).second)) {
            v.push_back(v2.at(k2++));
          } else {
            break;
          }
        }
        *v1 = v;
      })
      ->SetName("find topk");

  Context::mapjoin(topk, topk, // print top 10
                   [](const TopK &topk, Output<int, int> *o) {
                     CHECK_EQ(topk.vertices.size(), 10);
                     LOG(INFO) << "Top K:";
                     for (int i = 0; i < 10; i++) {
                       LOG(INFO) << "vertex: " << topk.vertices.at(i).first
                                 << "  pr: " << topk.vertices.at(i).second;
                     }
                     o->Add(0, 0);
                   },
                   [](TopK *topk, int) {})
      ->SetName("print topk");

  Context::write(topk, FLAGS_topk_url,
                 [](const TopK &topk, std::stringstream &ss) {
                   CHECK_EQ(topk.vertices.size(), 10);
                   for (int i = 0; i < 10; i++) {
                     ss << topk.vertices.at(i).first << "  "
                        << topk.vertices.at(i).second << "\n";
                   }
                 });
  // Context::count(loaded_dataset);
  // Context::count(vertex);

  // Context::count(vertex);
  Runner::Run();
}
