#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "", "The namenode of hdfs");
DEFINE_int32(hdfs_port, -1, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

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

int main(int argc, char** argv) {
  Runner::Init(argc, argv);

  // load and generate two collections?
  auto dataset = Context::load(FLAGS_url, [](std::string& s) {
    Links obj;
    
    std::stringstream ss(s);
    std::istream_iterator<std::string> begin(ss);
    std::istream_iterator<std::string> end;
    std::vector<std::string> split(begin, end);

    std::vector<std::string>::iterator it = split.begin();
    obj.vertex_id = std::stoi(*it);
    for ( it += 2; it != split.end(); ++it) {
      obj.outlinks.push_back(std::stoi(*it));
    }

    return obj;
  });

  int num_part = 10;
  auto vertex = Context::placeholder<Vertex>(num_part);
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
    });
 
  auto links = Context::placeholder<Links>(num_part);
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
    });
  
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
              kvs.push_back({outlink, iter1->pr});
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
  ->SetIter(5)
  ->SetStaleness(2)
  ->SetCombine([](float a, float b){
    return a+b;
  });
  

  Context::count(dataset);
  Runner::Run();
}
