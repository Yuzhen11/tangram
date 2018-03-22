#include <tuple>

#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "base/color.hpp"
#include "boost/tokenizer.hpp"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "", "The namenode of hdfs");
DEFINE_int32(hdfs_port, -1, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

DEFINE_string(url, "", "The url for hdfs file");

using namespace xyz;

struct Vertex {
  using KeyT = int;
  
  KeyT id;
  std::string label;
  std::vector<Vertex> outlinks;
  bool is_matched = false;

  Vertex() = default;
  Vertex(KeyT id) : id(id) {}
  Vertex(KeyT id, std::string label) : id(id), label(label) {} 
  KeyT Key() const { return id; }
  
  void AddOutlink(Vertex v) { outlinks.push_back(v); }
  
  std::deque<Vertex> GetRound(int round) {
    std::deque<Vertex> result;
    result.push_back(*this);
    if (round == 0) return result;
    for (int i = 0; i < round; i++) {
      int size = result.size();
      for (int j = 0; j < size; j++) {
        Vertex v = result.front();
        result.pop_front();
        for (Vertex outlink : v.outlinks) {
          result.push_back(outlink);
        }
      }
    }
    return result;
  }

  int GetDepth() {
    if (outlinks.empty()) return 1;
    int max = outlinks[0].GetDepth();
    for ( int i = 1; i < outlinks.size(); i++ ) {
      int depth = outlinks[i].GetDepth();
      if (depth > max) max = depth;
    }
    return max+1;
  }

  void Display() const {
    std::deque<Vertex> to_display;
    to_display.push_back(*this);
    while(!to_display.empty()) {
      Vertex v = to_display.front();
      to_display.pop_front(); 
      std::stringstream ss;
      ss << "(" << v.id << ", " << v.label << ") ->";
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

struct Matcher {
  using KeyT = int;

  KeyT id;
  int matched_round = -1;
  bool is_match_failed = false;
  std::map<int, std::map<int, std::vector<Vertex>>> result; // round, pattern pos, vertices 
  
  Matcher() = default;
  Matcher(int id) : id(id) {}
  KeyT Key() const { return id; }

  void CheckRound(Vertex pattern) {
    CHECK(matched_round >= 0);
    if (result[matched_round].size() != pattern.GetRound(matched_round).size()){
      is_match_failed = true;
      //LOG(INFO) << "matcher " << id << " failed at round " << matched_round;
      //LOG(INFO) << result[matched_round].size() << " " << pattern.GetRound(matched_round).size();
      matched_round -= 1;
    } else {
      //LOG(INFO) << "matcher " << id << " round " << matched_round; 
    }
  }
  
  void Display() {// find all matched pattern
    //TODO
    std::stringstream ss;
    ss << "[match pattern]\n";
    for ( auto round : result ) {
      ss << " round: " << round.first << "\n";
      for ( auto pos : round.second ) {
        ss << " pos: " << pos.first;
        for ( auto vertex : pos.second ) {
         ss << " (" << vertex.id << ", " << vertex.label << ")";
        }
        ss << "\n";
      }
    }
    LOG(INFO) << ss.str();
  }

  friend SArrayBinStream& operator << (SArrayBinStream& stream, const Matcher& matcher) {
    stream << matcher.id << matcher.matched_round << matcher.result;
  }
  friend SArrayBinStream& operator >> (SArrayBinStream& stream, Matcher& matcher) {
    stream >> matcher.id >> matcher.matched_round >> matcher.result;
  }
};

int main(int argc, char** argv) {
  Runner::Init(argc, argv);

  // define pattern to match
  // only tree, no loop
  Vertex pattern(0, "a");
  pattern.AddOutlink(Vertex(1, "b"));
  pattern.AddOutlink(Vertex(2, "c"));
  //pattern.outlinks.at(0).AddOutlink(Vertex(3, "d"));
  LOG(INFO) << "pattern:";
  pattern.Display();
  int iteration = pattern.GetDepth();

  // dataset from file
  auto dataset = Context::load(FLAGS_url, [](std::string& s) {
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();

    int id = std::stoi(*(it++));
    std::string label = *(it++);
    Vertex obj(id, label);
    while (it != tok.end()) {
      id = std::stoi(*(it++));
      label = *(it++);
      obj.AddOutlink(Vertex(id, label));  
    }
    return obj;
  });
 
  //vertices without outlinks considered
  auto graph = Context::placeholder<Vertex>(1);
  Context::mapjoin(dataset, graph,
    [](const Vertex& vertex){
      using MsgT = std::pair<int, std::pair<std::string, std::vector<Vertex>>>;
      std::vector<MsgT> kvs;
      kvs.push_back(std::make_pair(vertex.id, std::make_pair(vertex.label, vertex.outlinks)));
    
      std::vector<Vertex> empty;
      for (Vertex outlink : vertex.outlinks) {
        kvs.push_back(std::make_pair(outlink.id, std::make_pair(outlink.label, empty)));
      }
      return kvs;
    },
    [](Vertex* vertex, std::pair<std::string, std::vector<Vertex>> msg){
      vertex->label = msg.first;
      for (auto outlink : msg.second)
        vertex->AddOutlink(outlink);
    })
  ->SetCombine([](std::pair<std::string, std::vector<Vertex>>* msg1, std::pair<std::string, std::vector<Vertex>> msg2){
        CHECK_EQ(msg1->first, msg2.first);
        for (Vertex vertex : msg2.second) msg1->second.push_back(vertex);
      });

  //TODO: vertices without outlinks not considered -> pattern round>0
  auto matcher = Context::placeholder<Matcher>(5);
  Context::mapjoin(dataset, matcher,
      [](const Vertex& vertex){
        std::vector<std::pair<int, int>> msg;
        msg.push_back(std::make_pair(vertex.id, 0));
        for ( Vertex outlink : vertex.outlinks ) {
          msg.push_back(std::make_pair(outlink.id, 0));
        }
        return msg;
      },
      [](Matcher* matcher, int msg){
      })
  ->SetCombine([](int* msg1, int msg2){});

  using MsgT = std::pair<int, std::vector<std::tuple<int, int, Vertex>>>;//matcher id, (round id, postition id, vertex)
  Context::mappartwithjoin(matcher, graph, matcher,
      [&pattern](TypedPartition<Matcher>* p,
        TypedCache<Vertex>* typed_cache,
        AbstractMapProgressTracker* t){
        std::vector<MsgT> kvs;
      
        auto part = typed_cache->GetPartition(0);
        auto* with_p = static_cast<IndexedSeqPartition<Vertex>*>(part.get());
        CHECK(with_p->GetSize());
        for (auto matcher : *p) {
          if ( matcher.is_match_failed ) continue;
          
          MsgT MSG;
          MSG.first = matcher.id;
          
          if ( matcher.matched_round == -1 ) {
            Vertex* root = with_p->Find(matcher.id); // BLOCKED IF CANNOT FIND
            if ( root->label != pattern.label ) { // root matching failed
              kvs.push_back(MSG);// send empty msg
              continue;
            }
            else {
              MSG.second.push_back(std::make_tuple(0, 0, *root));
              kvs.push_back(MSG);
              continue; 
            } 
          }
        
          // root matched
          int next_round_pos_start = 0;
          auto pattern_round = pattern.GetRound(matcher.matched_round);
          for (int round_pos = 0; round_pos < pattern_round.size(); round_pos++){
            std::vector<Vertex> vertices_matcher = matcher.result[matcher.matched_round][round_pos];// round, pattern pos, vertices
            Vertex vertex_pattern = pattern_round.at(round_pos);
            if (round_pos != 0) {
              next_round_pos_start += pattern_round.at(round_pos-1).outlinks.size();
            }

            for (Vertex vertex_matcher : vertices_matcher) {

              for ( Vertex outlink : vertex_matcher.outlinks ) {

                for (int pos = 0; pos < vertex_pattern.outlinks.size(); pos++) {
                  
                  int next_round_pos = pos + next_round_pos_start;
                  if (outlink.label == vertex_pattern.outlinks.at(pos).label) { //outlink.outlinks here is empty
                    Vertex* vertex_to_add = with_p->Find(outlink.id);// BLOCKED IF CANNOT FIND
                    MSG.second.push_back(std::make_tuple(matcher.matched_round+1, next_round_pos, *vertex_to_add));//may repetitively push a vertex   
                  }
                }
              }
            } 
          }
          if (!MSG.second.empty()) kvs.push_back(MSG); // do not send empty msg
        }
        typed_cache->ReleasePart(0);
        return kvs;
      },
      [iteration, &pattern](Matcher* matcher, std::vector<std::tuple<int, int, Vertex>> msgs){
        if ( msgs.empty() ) {
          CHECK_EQ(matcher->matched_round, -1);
          CHECK_EQ(matcher->is_match_failed, false);
          matcher->is_match_failed = true;
          return;
        }
        
        for ( auto msg : msgs ) {
          matcher->result[std::get<0>(msg)][std::get<1>(msg)].push_back(std::get<2>(msg));
        }
       
        matcher->matched_round ++;
        matcher->CheckRound(pattern); //round-- if check failed
        
        if (matcher->matched_round == iteration-1)
          matcher->Display(); //print matching result
      })
  ->SetIter(iteration);

  Runner::Run();
}

