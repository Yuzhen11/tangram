#include "core/program_context.hpp"
#include "core/plan/mapjoin.hpp"
#include "core/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "", "The namenode of hdfs");
DEFINE_int32(hdfs_port, -1, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

DEFINE_string(url, "", "The url for hdfs file");

namespace xyz {

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

void Run() {
  // 1. construct the plan
  Collection<Vertex, SeqPartition<Vertex>> c1{1};
  c1.Load(FLAGS_url, [](std::string& s) {
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

  Collection<Vertex, IndexedSeqPartition<Vertex>> c2{2, 100};
  c2.SetMapper(std::make_shared<HashKeyToPartMapper<int>>(100));
  auto plan1 = GetMapJoin<std::vector<int>>(0, &c1, &c2);
  plan1.map = [](const Vertex& v) {
    return std::pair<int, std::vector<int>> (v.vertex, v.outlinks);
  };
  plan1.join = [](Vertex* v, std::vector<int> msgs) {
    for (auto msg : msgs) {
      v->outlinks.push_back(msg);
    }
    v->pr = 0.15;
  };  


  auto plan2 = GetMapJoin<float>(1, &c2, &c2);
  plan2.map_vec = [](const Vertex& v) {
    std::vector<std::pair<int, float>> contribs;
    for (auto outlink : v.outlinks) {
      contribs.push_back(std::pair<int, float>(outlink, v.pr/v.outlinks.size()));
    }
    return contribs;
  };
  plan2.join = [](Vertex* v, float m) {
    v->pr += 0.85 * m;
  };

  ProgramContext program;
  auto plan_spec1 = plan1.GetPlanSpec();
  auto plan_spec2 = plan2.GetPlanSpec();
  plan_spec2.num_iter = 10;
  program.plans.push_back(plan_spec1);
  program.plans.push_back(plan_spec2);
  program.collections.push_back(c1.GetSpec());
  program.collections.push_back(c2.GetSpec());

  // 2. create engine and register the plan
  Engine::Config config;
  config.scheduler = FLAGS_scheduler;
  config.scheduler_port = FLAGS_scheduler_port;
  config.num_threads = FLAGS_num_local_threads;
  config.namenode = FLAGS_hdfs_namenode;
  config.port = FLAGS_hdfs_port;

  Engine engine;
  // initialize the components and actors,
  // especially the function_store, to be registed by the plan
  engine.Init(config);
  // register program containing plan and collection info
  engine.RegisterProgram(program);
  // add related functions
  engine.AddFunc(plan1);
  engine.AddFunc(plan2);
  engine.AddFunc(c1);
  engine.AddFunc(c2);

  // start the mailbox and start to receive messages
  engine.Start();
  // stop the mailbox and actors
  engine.Stop();
}

}  // namespace xyz

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  CHECK(!FLAGS_scheduler.empty());

  xyz::Run();
}
