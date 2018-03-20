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

  auto c1 = Context::load(FLAGS_url, [](std::string& s) {
    Vertex v;
    
    std::stringstream ss(s);
    std::istream_iterator<std::string> begin(ss);
    std::istream_iterator<std::string> end;
    std::vector<std::string> split(begin, end);

    std::vector<std::string>::iterator it = split.begin();
    v.vertex = std::stoi(*it);
    for ( it += 2; it != split.end(); ++it) {
      v.outlinks.push_back(std::stoi(*it));
    }

    return v;
  });

  auto c2 = Context::placeholder<Vertex>(100);

  auto p1 = Context::mapjoin(c1, c2,
    [](const Vertex& v) {
      return std::pair<int, std::vector<int>> (v.vertex, v.outlinks);
    },
    [](Vertex* v, std::vector<int> outlinks) {
      for (auto outlink : outlinks) {
        v->outlinks.push_back(outlink);
      }
      v->pr = 0.15;
    });
  auto p2 = Context::mapjoin(c2, c2,
    [](const Vertex& v) {
      std::vector<std::pair<int, float>> contribs;
      for (auto outlink : v.outlinks) {
        contribs.push_back(std::pair<int, float>(outlink, v.pr/v.outlinks.size()));
      }
      return contribs;
    }, [](Vertex* v, float contrib) {
      v->pr += 0.85 * contrib;
    })->SetIter(5)->SetStaleness(2);
  p2->combine = [](float* a, float b) {
    *a = *a + b;
  };

  Context::count(c1);
  Runner::Run();
}
