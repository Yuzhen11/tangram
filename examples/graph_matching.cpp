#include <tuple>

#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "base/color.hpp"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "", "The namenode of hdfs");
DEFINE_int32(hdfs_port, -1, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

DEFINE_string(url, "", "The url for hdfs file");

using namespace xyz;

struct Vertex {
  using KeyT = int;

  Vertex() = default;
  Vertex(KeyT id) : id(id) {}
  Vertex(KeyT id, std::string label) : id(id), label(label) {} 
  KeyT Key() const { return id; }
  
  KeyT id;
  std::string label;
  std::vector<Vertex> outlinks;
  int round = 0;
  int matched_round = 0;
  bool match_failed = false;

  void AddOutlink(Vertex outlink) {
    outlink.round = round + 1;
    outlinks.push_back(outlink);
  }
  
  Vertex& GetOutlink(int i) {
    return outlinks.at(i);
  }
 
  bool FindLabel(std::string label) {
    for ( auto outlink : outlinks ) {
      if (outlink.label == label)
        return true;
    }
    return false;
  }

  Vertex Get(std::string label) {
    for ( auto outlink : outlinks) {
      if (outlink.label == label)
        return outlink;
    }
    return Vertex(-1);
  }
 
  std::deque<Vertex> GetRound(int i) {
    CHECK(i >= 0);
    std::deque<Vertex> result;
    result.push_back(*this);
    if (i) {
      for (int k = 0; k < i; k++) {
        int size = result.size();
        for (int j = 0; j < size; j++) {
          Vertex outlink = result.front();
          result.pop_front(); 
          for (Vertex tmp : outlink.outlinks) {
            result.push_back(tmp);
          }
        }
      } 
    }
    CHECK_NE(result.size(), 0);
    return result;
  }

  Vertex* GetById(int id) {
    if (this->id == id) return this;
    else {
      for (Vertex outlink : outlinks) return outlink.GetById(id);
    }
    return nullptr;
  }
  
  void Display() const {
    std::deque<Vertex> to_display;
    to_display.push_back(*this);
    while(!to_display.empty()) {
      Vertex v = to_display.front();
      to_display.pop_front(); 
      std::stringstream ss;
      ss << "round " << v.round << ": (" << v.id << ", " << v.label << ") ->";
      for (Vertex adj : v.outlinks) {
        to_display.push_back(adj); 
        ss << " (" << adj.id << ", " << adj.label << ")";
      }
      LOG(INFO) << ss.str();
    }
  }

  friend SArrayBinStream& operator << (SArrayBinStream& stream, const Vertex& vertex) {
    stream << vertex.id << vertex.label;
    stream << vertex.outlinks;
  }
  friend SArrayBinStream& operator >> (SArrayBinStream& stream, Vertex& vertex) {
    stream >> vertex.id >> vertex.label;
    stream >> vertex.outlinks;
  }
};

int main(int argc, char** argv) {
  Runner::Init(argc, argv);

  // define pattern to match
  // only tree, no loop
  Vertex pattern(0, "a");
  pattern.AddOutlink(Vertex(1, "b"));
  pattern.AddOutlink(Vertex(2, "c"));
  //pattern.outlinks.at(0).AddOutlink(Vertex(3, "b"));
  LOG(INFO) << "pattern:";
  pattern.Display();
  int iteration = 1;

  // dataset from file
  auto dataset = Context::load(FLAGS_url, [](std::string& s) {
    std::stringstream ss(s);
    std::istream_iterator<std::string> begin(ss);
    std::istream_iterator<std::string> end;
    std::vector<std::string> split(begin, end);

    std::vector<std::string>::iterator it = split.begin();
    int id = std::stoi(*(it++));
    std::string label = *(it++);
    Vertex obj(id, label);
    for ( ; it != split.end(); ) {
      id = std::stoi(*(it++));
      label = *(it++);
      obj.AddOutlink(Vertex(id, label));  
    }
    return obj;
  });
  
  auto graph = Context::placeholder<Vertex>(1);
  Context::mapjoin(dataset, graph,
    [](const Vertex& vertex){
      return std::make_pair(vertex.id, std::make_pair(vertex.label, vertex.outlinks));
    },
    [](Vertex* vertex, std::pair<std::string, std::vector<Vertex>> msg){
      vertex->label = msg.first;
      for (auto outlink : msg.second)
        vertex->AddOutlink(outlink);
    });

  int num_part = 10;
  auto matcher = Context::placeholder<Vertex>(num_part);
  Context::mapjoin(dataset, matcher,
      [](const Vertex& vertex){
        return std::make_pair(vertex.id, vertex.label);
      },
      [](Vertex* matcher, std::string msg){
        matcher->label = msg;
      });
  
  Context::mappartwithjoin(matcher, graph, matcher,
      [&pattern](TypedPartition<Vertex>* p,
        TypedCache<Vertex>* typed_cache,
        AbstractMapProgressTracker* t){
        std::vector<std::pair<int, std::vector< std::pair<int, std::vector<Vertex>> >>> kvs;
      
        auto part = typed_cache->GetPartition(0);
        auto* with_p = static_cast<IndexedSeqPartition<Vertex>*>(part.get());
        CHECK(with_p->GetSize());
        for (auto matcher : *p) {
          std::vector< std::pair<int, std::vector<Vertex>> > MSG;
          
          if (matcher.match_failed) continue;//match failed and skip
          if ((matcher.matched_round == 0) && (matcher.label != pattern.label)) {//root match failed
            kvs.push_back(std::make_pair(matcher.id, MSG));
            continue;
          }
          
          //root matched
          auto sub_matcher = matcher.GetRound(matcher.matched_round);
          auto sub_pattern = pattern.GetRound(matcher.matched_round);
          CHECK_EQ(sub_matcher.size(), sub_pattern.size()) << matcher.matched_round;
          for (int i = 0; i < sub_matcher.size(); i++){
            if (sub_pattern.at(i).outlinks.size() == 0) continue;
            std::pair<int, std::vector<Vertex>> msg;
            msg.first = sub_matcher.at(i).id;
            MSG.push_back(msg);
            auto v = with_p->Find(sub_matcher.at(i).id);
            for (Vertex next : sub_pattern.at(i).outlinks) {
              Vertex candidate = v->Get(next.label);//TODO

              if ( candidate.id == -1 ){// match failed
                MSG.clear();//empty - this matcher failed
                break;
              } else {
                MSG.back().second.push_back(candidate); 
              }
            }
          }
          kvs.push_back(std::make_pair(matcher.id, MSG));
        }
        typed_cache->ReleasePart(0);
        return kvs;
      },
      [iteration](Vertex* matcher, std::vector< std::pair<int, std::vector<Vertex>> > MSG){
        if (MSG.empty()) {
          matcher->match_failed = true;
          return;
        }

        for (auto msg : MSG) {
          Vertex* to_be_added = matcher->GetById(msg.first);
          CHECK(to_be_added != nullptr);
          for (Vertex to_add : msg.second) {
            to_be_added->AddOutlink(to_add);
          }
        }

        matcher->matched_round ++;
        if (matcher->matched_round == iteration) matcher->Display(); //print matching result
      })
  ->SetIter(iteration);

  Runner::Run();
}

