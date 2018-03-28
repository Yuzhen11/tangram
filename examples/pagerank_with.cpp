#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "boost/tokenizer.hpp"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "", "The namenode of hdfs");
DEFINE_int32(hdfs_port, -1, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");
DEFINE_int32(num_parts, 100, "# num of partitions");

DEFINE_string(url, "", "The url for hdfs file");

using namespace xyz;

struct Vertex {
  using KeyT = int;

  Vertex() : pr(0.15) {};
  Vertex(KeyT vertex) : vertex_id(vertex), pr(0.15) {}
  KeyT Key() const { return vertex_id; }

  KeyT vertex_id;
  float pr;

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Vertex& vertex) {
    stream << vertex.vertex_id << vertex.pr;
    return stream; 
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Vertex& vertex) {
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

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Links& links) {
    stream << links.vertex_id << links.outlinks;
    return stream; 
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Links& links) {
    stream >> links.vertex_id >> links.outlinks;
    return stream; 
  }

};

struct TopK {
  using KeyT = int;
  TopK() = default;
  TopK(KeyT id) : id(id) {}
  KeyT Key()  const { return id; }
  KeyT id;
  std::vector<Vertex> vertices;

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const TopK& topk) {
    stream << topk.id << topk.vertices;
    return stream; 
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, TopK& topk) {
    stream >> topk.id >> topk.vertices;
    return stream; 
  }
};

int main(int argc, char** argv) {
  Runner::Init(argc, argv);

  auto dataset = Context::load(FLAGS_url, [](std::string& s) {//s cannot be empty 
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
    int id = stoi(*it++);
    it++;
    Links obj(id);
    while (it != tok.end()) {
      obj.outlinks.push_back(std::stoi(*it++));
    }

    return obj;
  })->SetName("dataset");

  auto vertex = Context::placeholder<Vertex>(FLAGS_num_parts)->SetName("vertex");
  auto p1 = Context::mapjoin(dataset, vertex,
    [](const Links& obj) {
      std::vector<std::pair<int,int>> all;
      all.push_back(std::make_pair(obj.vertex_id, 0));
      for (auto outlink : obj.outlinks) {
        all.push_back(std::make_pair(outlink, 0));
      }
      return all;
    },
    [](Vertex* v, int msg) {
      v->pr = 0.15;
    })
    ->SetCombine([](int* msg1, int msg2){})
    ->SetName("construct vertex from dataset");
 
  auto links = Context::placeholder<Links>(FLAGS_num_parts)->SetName("links");
  auto p2 = Context::mapjoin(dataset, links,
    [](const Links& obj) {
      std::vector<std::pair<int, std::vector<int>>> all;
      all.push_back(std::make_pair(obj.vertex_id, obj.outlinks));
      std::vector<int> empty;
      for (auto outlink : obj.outlinks) {
        all.push_back(std::make_pair(outlink, empty));
      }
      return all;
    },
    [](Links* links, std::vector<int> outlinks) {
      for (auto outlink : outlinks) {
        links->outlinks.push_back(outlink);
      }
    })
  ->SetCombine([](std::vector<int>* msg1, std::vector<int> msg2){
    for (int value : msg2) msg1->push_back(value); 
  })
  ->SetName("construct links from dataset");

  Context::sort_each_partition(vertex);
  Context::sort_each_partition(links);
  
  auto p = Context::mappartwithjoin(vertex, links, vertex,
      [](TypedPartition<Vertex>* p,
        TypedCache<Links>* typed_cache,
        AbstractMapProgressTracker* t){
          std::vector<std::pair<int, float>> kvs;
          auto part = typed_cache->GetPartition(p->id);
          auto* with_p = static_cast<TypedPartition<Links>*>(part.get());
         
          CHECK_EQ(p->GetSize(), with_p->GetSize());
          auto iter1 = p->begin();
          auto iter2 = with_p->begin();
          while(iter1 != p->end()) {
            CHECK_EQ(iter1->vertex_id, iter2->vertex_id);
            for (auto outlink: iter2->outlinks){
              kvs.push_back({outlink, iter1->pr/iter2->outlinks.size()});
            }
            ++iter1;
            ++iter2;
          }
        typed_cache->ReleasePart(p->id);
        return kvs;
      },
      [](Vertex* vertex, float m){
        vertex->pr += 0.85*m;
      })
  ->SetCombine([](float* a, float b){
   *a = *a + b; 
   })
  ->SetIter(10)
  ->SetStaleness(2)
  ->SetName("pagerank main logic");

  auto topk = Context::placeholder<TopK>(1)->SetName("topk");
  Context::mapjoin(vertex, topk,
      [](const Vertex& vertex){
        std::vector<Vertex> vertices;
        vertices.push_back(vertex);
        return std::make_pair(0, vertices);
      },
      [](TopK* topk, const std::vector<Vertex>& vertices){
        std::vector<Vertex> v;
        int k1 = 0;
        int k2 = 0;
        for (int i = 0; i < 10; i++) { // top 10
          if (k1 != topk->vertices.size() && (k2 == vertices.size() || topk->vertices.at(k1).pr > vertices.at(k2).pr)) {
            v.push_back(topk->vertices.at(k1++));
          }
          else if (k2 != vertices.size() && (k1 == topk->vertices.size() || topk->vertices.at(k1).pr <= vertices.at(k2).pr)) {
            v.push_back(vertices.at(k2++));
          } else { break; }
        }
        topk->vertices = v;
      })
    ->SetCombine([](std::vector<Vertex>* v1, const std::vector<Vertex>& v2){
        std::vector<Vertex> v;
        int k1 = 0;
        int k2 = 0;
        for (int i = 0; i < 10; i++) { // top 10
          if (k1 != v1->size() && (k2 == v2.size() || v1->at(k1).pr > v2.at(k2).pr)) {
            v.push_back(v1->at(k1++));
          }
          else if (k2 != v2.size() && (k1 == v1->size() || v1->at(k1).pr <= v2.at(k2).pr)) {
            v.push_back(v2.at(k2++));
          } else { break; }
        }
        *v1 = v; 
      })
    ->SetName("find topk");

  Context::mapjoin(topk, topk, // print top 10
      [](const TopK& topk){
        CHECK_EQ(topk.vertices.size(), 10);
        LOG(INFO) << "Top K:";
        for (int i = 0; i < 10; i ++) {
          LOG(INFO) << "vertex: " <<topk.vertices.at(i).vertex_id << "  pr: " << topk.vertices.at(i).pr;
        }
        return std::make_pair(0,0);
      },
      [](TopK* topk, int){}
      )
  ->SetName("print topk");
  
  //Context::count(dataset);
  Runner::Run();
}
