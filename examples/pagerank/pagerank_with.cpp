#include "boost/tokenizer.hpp"
#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

#define ENABLE_CP

DEFINE_int32(num_parts, 100, "# num of partitions");
DEFINE_string(url, "", "The url for hdfs file");
DEFINE_string(combine_type, "kDirectCombine",
              "kShuffleCombine, kDirectCombine, kNoCombine, timeout");

using namespace xyz;

struct Vertex {
  using KeyT = int;

  Vertex() : pr(0.15){};
  Vertex(KeyT vertex) : vertex_id(vertex), pr(0.15) {}
  KeyT Key() const { return vertex_id; }

  KeyT vertex_id;
  float pr;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Vertex &vertex) {
    stream << vertex.vertex_id << vertex.pr;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Vertex &vertex) {
    stream >> vertex.vertex_id >> vertex.pr;
    return stream;
  }
};

struct Links {
  using KeyT = int;

  Links() = default;
  Links(KeyT vertex) : vertex_id(vertex) {}
  KeyT Key() const { return vertex_id; }
  KeyT vertex_id;
  std::vector<int> outlinks;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Links &links) {
    stream << links.vertex_id << links.outlinks;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Links &links) {
    stream >> links.vertex_id >> links.outlinks;
    return stream;
  }
};

struct TopK {
  using KeyT = int;
  TopK() = default;
  TopK(KeyT id) : id(id) {}
  KeyT Key() const { return id; }
  KeyT id;
  std::vector<Vertex> vertices;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const TopK &topk) {
    stream << topk.id << topk.vertices;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream, TopK &topk) {
    stream >> topk.id >> topk.vertices;
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

  auto dataset =
      Context::load(FLAGS_url, [](std::string s) { // s cannot be empty
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(s, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it =
            tok.begin();
        int id = stoi(*it++);
        it++;
        Links obj(id);
        while (it != tok.end()) {
          obj.outlinks.push_back(std::stoi(*it++));
        }

        return obj;
      })->SetName("dataset");

  auto vertex =
      Context::placeholder<Vertex>(FLAGS_num_parts)->SetName("vertex");
  auto p1 =
      Context::mappartupdate(dataset, vertex,
                           [](TypedPartition<Links> *p, Output<int, int> *o) {
                             for (auto &v : *p) {
                               o->Add(v.vertex_id, 0);
                               for (auto outlink : v.outlinks) {
                                 o->Add(outlink, 0);
                               }
                             }
                           },
                           [](Vertex *v, int msg) { v->pr = 0.15; })
          ->SetCombine([](int *msg1, int msg2) {})
          ->SetName("construct vertex from dataset");

  auto links = Context::placeholder<Links>(FLAGS_num_parts)->SetName("links");
  auto p2 = Context::mappartupdate(
                dataset, links,
                [](TypedPartition<Links> *p, Output<int, std::vector<int>> *o) {
                  for (auto &v : *p) {
                    o->Add(v.vertex_id, v.outlinks);
                    for (auto outlink : v.outlinks) {
                      o->Add(outlink, std::vector<int>());
                    }
                  }
                },
                [](Links *links, std::vector<int> outlinks) {
                  for (auto outlink : outlinks) {
                    links->outlinks.push_back(outlink);
                  }
                })
                ->SetCombine([](std::vector<int> *msg1, std::vector<int> msg2) {
                  for (int value : msg2)
                    msg1->push_back(value);
                })
                ->SetName("construct links from dataset");

  Context::sort_each_partition(vertex);
  Context::sort_each_partition(links);

#ifdef ENABLE_CP
  Context::checkpoint(vertex, "/tmp/tmp/yz");
  Context::checkpoint(links, "/tmp/tmp/yz");
#endif

  auto p =
      Context::mappartwithupdate(
          vertex, links, vertex,
          [](TypedPartition<Vertex> *p, TypedCache<Links> *typed_cache,
             Output<int, float> *o) {
            auto part = typed_cache->GetPartition(p->id);
            auto *with_p = static_cast<TypedPartition<Links> *>(part.get());

            CHECK_EQ(p->GetSize(), with_p->GetSize());
            auto iter1 = p->begin();
            auto iter2 = with_p->begin();
            while (iter1 != p->end()) {
              CHECK_EQ(iter1->vertex_id, iter2->vertex_id);
              for (auto outlink : iter2->outlinks) {
                o->Add(outlink, iter1->pr / iter2->outlinks.size());
              }
              ++iter1;
              ++iter2;
            }
            typed_cache->ReleasePart(p->id);
          },
          [](Vertex *vertex, float m) { vertex->pr += 0.85 * m; })
          ->SetCombine([](float *a, float b) { *a = *a + b; }, combine_timeout)
          ->SetIter(25)
          ->SetStaleness(0)
#ifdef ENABLE_CP
          ->SetCheckpointInterval(5, "/tmp/tmp/yz")
#endif
          ->SetName("pagerank main logic");

  auto topk = Context::placeholder<TopK>(1)->SetName("topk");
  Context::mapupdate(
      vertex, topk,
      [](const Vertex &vertex, Output<int, std::vector<Vertex>> *o) {
        std::vector<Vertex> vertices;
        vertices.push_back(vertex);
        o->Add(0, std::move(vertices));
      },
      [](TopK *topk, std::vector<Vertex> vertices) {
        std::vector<Vertex> v;
        int k1 = 0;
        int k2 = 0;
        for (int i = 0; i < 10; i++) { // top 10
          if (k1 != topk->vertices.size() &&
              (k2 == vertices.size() ||
               topk->vertices.at(k1).pr > vertices.at(k2).pr)) {
            v.push_back(topk->vertices.at(k1++));
          } else if (k2 != vertices.size() &&
                     (k1 == topk->vertices.size() ||
                      topk->vertices.at(k1).pr <= vertices.at(k2).pr)) {
            v.push_back(vertices.at(k2++));
          } else {
            break;
          }
        }
        topk->vertices = v;
      })
      ->SetCombine([](std::vector<Vertex> *v1, const std::vector<Vertex> &v2) {
        std::vector<Vertex> v;
        int k1 = 0;
        int k2 = 0;
        for (int i = 0; i < 10; i++) { // top 10
          if (k1 != v1->size() &&
              (k2 == v2.size() || v1->at(k1).pr > v2.at(k2).pr)) {
            v.push_back(v1->at(k1++));
          } else if (k2 != v2.size() &&
                     (k1 == v1->size() || v1->at(k1).pr <= v2.at(k2).pr)) {
            v.push_back(v2.at(k2++));
          } else {
            break;
          }
        }
        *v1 = v;
      })
      ->SetName("find topk");

  Context::mapupdate(topk, topk, // print top 10
                   [](const TopK &topk, Output<int, int> *o) {
                     CHECK_EQ(topk.vertices.size(), 10);
                     LOG(INFO) << "Top K:";
                     for (int i = 0; i < 10; i++) {
                       LOG(INFO) << "vertex: " << topk.vertices.at(i).vertex_id
                                 << "  pr: " << topk.vertices.at(i).pr;
                     }
                   },
                   [](TopK *topk, int) {})
      ->SetName("print topk");

  // Context::count(dataset);
  Runner::Run();
}
