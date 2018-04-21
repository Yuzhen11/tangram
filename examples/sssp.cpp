#include <limits>
#include <algorithm>
#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "boost/tokenizer.hpp"

DEFINE_int32(num_parts, 100, "# num of partitions");
DEFINE_int32(sourceID, 0, "# num of partitions");
DEFINE_int32(iteration, 0, "# num of partitions");
DEFINE_string(url, "", "The url for hdfs file");
DEFINE_bool(display, false, "The url for hdfs file");
DEFINE_string(combine_type, "kDirectCombine", "kShuffleCombine, kDirectCombine, kNoCombine, timeout");

using namespace xyz;

struct Vertex
{
  using KeyT = int;

  Vertex() = default;
  Vertex(KeyT id) : id(id) {
	if (id == FLAGS_sourceID) {
	  distance = 0;
	  updated = true;
	}
  }
  KeyT Key() const { return id; }

  KeyT id;
  std::vector<int> outlinks;
  int distance = std::numeric_limits<int>::max();
  bool updated = false;

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Vertex& vertex) {
    stream << vertex.id << vertex.outlinks << vertex.distance;
    return stream; 
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Vertex& vertex) {
    stream >> vertex.id >> vertex.outlinks >> vertex.distance;
    return stream; 
  }
};


int main(int argc, char** argv) {
  Runner::Init(argc, argv);
  const int combine_timeout = ParseCombineTimeout(FLAGS_combine_type);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "combine_type: " << FLAGS_combine_type << ", timeout: " << combine_timeout;
  }

  auto loaded_dataset = Context::load(FLAGS_url, [](std::string s) {
    Vertex v;
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();

    v.id = std::stoi(*it++);
    it++;
    while (it != tok.end()) {
      v.outlinks.push_back(std::stoi(*it++));
    }

    return v;
  })->SetName("dataset");

  auto vertex = Context::placeholder<Vertex>(FLAGS_num_parts)->SetName("vertex");

  const int sourceID = FLAGS_sourceID;
  Context::mappartjoin(loaded_dataset, vertex, 
    [](TypedPartition<Vertex>* p,
      AbstractMapProgressTracker* t) {
      std::vector<std::pair<int, std::vector<int>>> all;
      for (auto& v: *p) {
        all.push_back({v.id, v.outlinks});
        for (auto outlink: v.outlinks) {
          all.push_back({outlink, std::vector<int>()});
        }
      }
      return all;
    },
    [sourceID](Vertex* v, std::vector<int> outlinks) {
	  if (v->id == sourceID) {
	    v->distance = 0;
		v->updated = true;
	  }
	  else {
	    v->distance = std::numeric_limits<int>::max();
	  }
      for (auto outlink : outlinks) {
        v->outlinks.push_back(outlink);
      }
    }
  )->SetCombine([](std::vector<int>* msg1, std::vector<int> msg2){
    for (int value : msg2) msg1->push_back(value); 
  })
  ->SetName("construct vertex");

  Context::sort_each_partition(vertex);

  auto p2 = Context::mappartjoin(vertex, vertex,
    [](TypedPartition<Vertex>* p,
      AbstractMapProgressTracker* t) {
      std::vector<std::pair<int, int>> contribs;
      for (auto& v: *p) {
	    if (v.updated) {
          for (auto outlink : v.outlinks) {
            contribs.push_back(std::pair<int, int>(outlink, v.distance));
          }
		  v.updated = false;
		}
      }
      return contribs;
    },
    [](Vertex* v, int contrib) {
	  if (contrib + 1 < v->distance) {
	    v->distance = contrib + 1;
		v->updated = true;
	  }
    }
  )->SetCombine([](int* msg1, int msg2) {
	*msg1 = std::min(*msg1, msg2);
  }, combine_timeout)
  ->SetIter(FLAGS_iteration)
  ->SetStaleness(0)
  ->SetName("sssp main logic");

  if (FLAGS_display) {
    Context::mappartjoin(vertex, vertex,
      [](TypedPartition<Vertex>* p,
        AbstractMapProgressTracker* t) {
        for (Vertex v : *p) {
          if (v.distance != std::numeric_limits<int>::max()) {
      	  LOG(INFO) << "(id, distance): " << v.id << ", " << v.distance;
      	}
        }
        return std::vector<std::pair<int,int>>();
      },
      [](Vertex* v, int contrib) {}
    )->SetName("display result");
  }

  Runner::Run();
}
