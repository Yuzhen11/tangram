#include <tuple>
#include <chrono>
#include <mutex>
#include <stdlib.h>
#include <cmath>

#include "core/plan/runner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "base/color.hpp"
#include "boost/tokenizer.hpp"
#include "core/index/hash_key_to_part_mapper.hpp"

DEFINE_int32(num_matcher_parts, 400, "# num of matcher partitions");
DEFINE_int32(num_graph_parts, 400, "# num of graph partitions");
DEFINE_int32(num_matchers, 10, "# num of graph partitions");
DEFINE_string(url, "", "The url for hdfs file");

using namespace xyz;

struct CountObj {
  using KeyT = int;
  
  KeyT id;
  int64_t count = 0;
  int64_t total_memory = 0;
  int64_t matched_memory = 0;

  CountObj() { id = 0; };
  CountObj(KeyT id) : id(id) {}
  KeyT Key() const { return id; }
 
  friend SArrayBinStream& operator << (SArrayBinStream& stream, const CountObj& obj) {
    stream << obj.id << obj.count;
  }
  friend SArrayBinStream& operator >> (SArrayBinStream& stream, CountObj& obj) {
    stream >> obj.id >> obj.count;
  } 
};

struct Vertex {
  using KeyT = int;
  KeyT id;
  char label;
  std::vector<Vertex> outlinks;
  Vertex() = default;
  Vertex(KeyT id) : id(id) {}
  Vertex(KeyT id, char label) : id(id), label(label) {}
  KeyT Key() const { return id; }

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Vertex id: " << id << ", label: " << label << ", outlinks: ";
    for (const Vertex& outlink : outlinks)
	  ss << "(" << outlink.id << ", " << outlink.label << ")";
    return ss.str();
  }

  friend SArrayBinStream& operator << (SArrayBinStream& stream, const Vertex& vertex) {
    stream << vertex.id << vertex.label << vertex.outlinks;
  }
  friend SArrayBinStream& operator >> (SArrayBinStream& stream, Vertex& vertex) {
    stream >> vertex.id >> vertex.label >> vertex.outlinks;
  }
};

struct Pattern {
  using KeyT = int;
  KeyT id;
  char label;
  std::vector<Pattern> outlinks;
  std::vector<Pattern> siblings;

  Pattern() = default;
  Pattern(KeyT id) : id(id) {}
  Pattern(KeyT id, char label) : id(id), label(label) {} 
  Pattern(Vertex v) : id(v.id), label(v.label) {}
  KeyT Key() const { return id; }
  
  void AddOutlink(Vertex v) { outlinks.push_back(Pattern(v)); }
  void AddOutlink(Pattern p) { outlinks.push_back(p); }
  void AddSibling(Vertex v) { siblings.push_back(Pattern(v)); }
  void AddSibling(Pattern p) { siblings.push_back(p); }

  Pattern* GetOutlink(int id) {
    for (auto& outlink : outlinks) {
      if (id == outlink.id) return &outlink;
    }
    return nullptr;
  }

  std::deque<Pattern> GetRound(int round) const {
    std::deque<Pattern> result;
    result.push_back(*this);
    if (round == 0) return result;
    for (int i = 0; i < round; i++) {
      int size = result.size();
      for (int j = 0; j < size; j++) {
        Pattern v = result.front();
        result.pop_front();
        for (auto outlink : v.outlinks) {
          result.push_back(outlink);
        }
      }
    }
    return result;
  }

  int GetDepth() const {
    if (outlinks.empty()) return 1;
    int max = 0;
    for (auto outlink : outlinks) {
      int depth = outlink.GetDepth();
      if (depth > max) max = depth;
    }
    return max+1;
  }

  std::string DebugString() const {
    std::deque<Pattern> to_display;
    to_display.push_back(*this);
	std::stringstream ss;
	ss << "Pattern:\n";
    while(!to_display.empty()) {
      Pattern v = to_display.front();
      to_display.pop_front(); 
      ss << "(" << v.id << ", " << v.label << ") -> outlinks:";
      for (auto outlink : v.outlinks) {
        to_display.push_back(outlink); 
        ss << " (" << outlink.id << ", " << outlink.label << ")";
      }
      ss << " siblings:";
      for (auto sibling : v.siblings) {
        ss << " (" << sibling.id << ", " << sibling.label << ")";
      }
	  ss << "\n";
    }
	return ss.str();
  }

  friend SArrayBinStream& operator << (SArrayBinStream& stream, const Pattern& pattern) {
    stream << pattern.id << pattern.label;
    stream << pattern.outlinks << pattern.siblings;
  }
  friend SArrayBinStream& operator >> (SArrayBinStream& stream, Pattern& pattern) {
    stream >> pattern.id >> pattern.label;
    stream >> pattern.outlinks >> pattern.siblings;
  }
};

struct Matcher {
  using KeyT = int;

  KeyT id;
  int matched_round = -1;
  std::set<int> seeds;

  //round, pattern pos, vertex id, label, parent ids
  std::map<int, std::map<int, std::map<int, std::pair<char, std::vector<int>>>>> result;
  std::map<int, std::set<int>> sibling_result; //vertex id, sibling ids.  
  Matcher() = default;
  Matcher(int id) : id(id) {}
  KeyT Key() const { return id; }

  void UpdateResult(Pattern pattern) {//TODO
	std::map<int, std::set<int>> childNum;
	std::set<int> toRemove;
	auto patternChildren = pattern.GetRound(matched_round);
    auto patternParents = pattern.GetRound(matched_round - 1);
	int patternChildStartPos = 0;
	for (int parentPos = 0; parentPos < patternParents.size(); parentPos++) {
	  if (parentPos != 0) patternChildStartPos += patternParents[parentPos-1].outlinks.size();
      for (int pos = 0; pos < patternParents[parentPos].outlinks.size(); pos++) {
		int childPos = pos + patternChildStartPos;
		for (auto tmp : result[matched_round][childPos]) {
		  for (int tmp2 : tmp.second.second) {
		    childNum[tmp2].insert(childPos);
		  }
		}
	  }
	}

	for (auto tmp1 : result[matched_round-1]) {
	  int patternPos = tmp1.first;
	  for (auto tmp2 : tmp1.second) {
		int vertexId = tmp2.first;
		if (childNum[vertexId].size() < patternParents[patternPos].outlinks.size()) {
		  toRemove.insert(vertexId);
		}
	  }
	}
	std::stringstream ss;
	ss << "Round: " << matched_round << ". Vertices in matcher " << id << " to delete: ";
	for (int tmp : toRemove) ss << tmp << ", ";

	for (auto tmp1 : result[matched_round-1]) {
	  for (auto tmp2 : tmp1.second) {
	    if (toRemove.find(tmp2.first) != toRemove.end()) {
		  tmp1.second.erase(tmp2.first);
		}
	  }
	}

	for (auto tmp1 : result[matched_round]) {
	  for (auto tmp2 : tmp1.second) {
		for (auto iter = tmp2.second.second.begin(); iter != tmp2.second.second.end(); iter++) {
		  if (toRemove.find(*iter) != toRemove.end()) {
			tmp2.second.second.erase(iter);
		  }
		}
	  }
	}
  }
  
  std::string DebugString() const {
    std::stringstream ss;
    ss << "[match pattern]\n";
    for (auto round : result) {
      ss << " round: " << round.first << "\n";
      for (auto pos : round.second) {
        ss << " pos: " << pos.first;
        for (auto vertex : pos.second) {
         ss << " (" << vertex.first << ", " << vertex.second.first << "; ";
         for (int id : vertex.second.second) ss << id << "; ";
         ss << ")";
        }
        ss << "\n";
      }
    }
	ss << "sibling result:\n";
	for (auto sibling : sibling_result) {
	  if (sibling.second.size() == 0) continue;
	  ss << "id: " << sibling.first << " -> ";
	  for (int id : sibling.second) ss << id << ", ";
	  ss << "\n";
	}
    return ss.str();
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
  const int num_graph_parts = FLAGS_num_graph_parts;
  const int num_matcher_parts = FLAGS_num_matcher_parts;
  const int num_matchers = FLAGS_num_matchers;

  // define pattern to match
  // only one sibling in pattern now
  Pattern pattern(0, 'a');
  pattern.AddOutlink(Vertex(1, 'b'));
  pattern.AddOutlink(Vertex(2, 'c'));
  pattern.GetOutlink(1)->AddSibling(Vertex(2, 'c'));
  pattern.GetOutlink(2)->AddSibling(Vertex(1, 'b'));
  pattern.GetOutlink(2)->AddOutlink(Vertex(3, 'b'));
  pattern.GetOutlink(2)->GetOutlink(3)->AddOutlink(Vertex(4, 'd'));
  LOG(INFO) << pattern.DebugString();
  int iteration = pattern.GetDepth();
  CHECK_EQ(iteration, 4);
  CHECK_EQ(pattern.GetRound(0).size(),1);
  CHECK_EQ(pattern.GetRound(1).size(),2);
  CHECK_EQ(pattern.GetRound(2).size(),1);
  CHECK_EQ(pattern.GetRound(3).size(),1);

  // dataset from file
  auto dataset = Context::load(FLAGS_url, [](std::string s) {
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();

    int id = std::stoi(*(it++));
    char label = *(it->begin());
	it++;
    Vertex obj(id, label);
    while (it != tok.end()) {
      id = std::stoi(*(it++));
      label = *(it->begin());
	  it++;
      obj.outlinks.push_back(Vertex(id, label));  
    }
    return obj;
  })->SetName("dataset");

  auto graph_key_part_mapper = std::make_shared<HashKeyToPartMapper<int>>(num_graph_parts);
  auto graph = Context::placeholder<Vertex>(num_graph_parts, graph_key_part_mapper);

  Context::mapjoin(dataset, graph,
    [](const Vertex& vertex){
      using MsgT = std::pair<int, std::pair<char, std::vector<Vertex>>>;
      std::vector<MsgT> kvs;
      kvs.push_back(std::make_pair(vertex.id, std::make_pair(vertex.label, vertex.outlinks)));
    
      std::vector<Vertex> empty;
      for (Vertex outlink : vertex.outlinks) {
        kvs.push_back(std::make_pair(outlink.id, std::make_pair(outlink.label, empty)));
      }
      return kvs;
    },
    [](Vertex* vertex, std::pair<char, std::vector<Vertex>> msg){
      vertex->label = msg.first;
      for (auto& outlink : msg.second) {
        vertex->outlinks.push_back(std::move(outlink));
      }
    }
  )->SetCombine([](std::pair<char, std::vector<Vertex>>* msg1, std::pair<char, std::vector<Vertex>> msg2){
    CHECK_EQ(msg1->first, msg2.first);
    for (Vertex& vertex : msg2.second) msg1->second.push_back(std::move(vertex));
  })
  ->SetName("graph");
  Context::sort_each_partition(graph);

  auto matcher = Context::placeholder<Matcher>(num_matcher_parts);
  Context::mapjoin(dataset, matcher,
    [num_matchers](const Vertex& vertex){
	  int matcherId = vertex.id % num_matchers;
      return std::make_pair(matcherId, vertex.id);//TODO: now put close seeds in one matcher
    },
    [](Matcher* matcher, int msg){
	  matcher->seeds.insert(msg);
    }
  )->SetName("matcher");

  //matcher id, (round id, postition id, vertex id, label, parent id, sibling ids)
  using MsgT = std::pair<int, std::vector<std::tuple<int, int, int, char, int, std::set<int>>>>;
  Context::mappartwithjoin(matcher, graph, matcher,
    [pattern, num_matcher_parts, num_graph_parts, graph_key_part_mapper, num_matchers](TypedPartition<Matcher>* p,
      TypedCache<Vertex>* typed_cache,
      AbstractMapProgressTracker* t){
      std::vector<MsgT> kvs;
   
      std::map<int, std::shared_ptr<IndexedSeqPartition<Vertex>>> with_parts;
	  int start_idx = rand()%num_graph_parts;
      for (int i = 0; i < num_graph_parts; i++) {
        int idx = (start_idx + i) % num_graph_parts;
        auto part = typed_cache->GetPartition(idx);
        with_parts[idx] = std::dynamic_pointer_cast<IndexedSeqPartition<Vertex>>(part);
      }

      for (auto matcher : *p) {
        MsgT MSG;
        MSG.first = matcher.id;
		if (matcher.matched_round == -1) {//first round
		  for (int seed : matcher.seeds) {
            auto with_p = with_parts[graph_key_part_mapper->Get(seed)];
            Vertex* root = with_p->Find(seed);
            CHECK(root != nullptr);
            if (root->label != pattern.label) {
              MSG.second.push_back(std::make_tuple(-1, -1, root->id, root->label, -1, std::set<int>()));
            }
            else {
              MSG.second.push_back(std::make_tuple(0, 0, root->id, root->label, -1, std::set<int>()));
            }
		  }
		}
		else {//except first round
          int next_round_pos_start = 0;
          auto pattern_round = pattern.GetRound(matcher.matched_round);
          for (int round_pos = 0; round_pos < pattern_round.size(); round_pos++){
            auto vertices_matcher = matcher.result[matcher.matched_round][round_pos];
            Pattern vertex_pattern = pattern_round.at(round_pos);
            if (round_pos != 0) next_round_pos_start += pattern_round.at(round_pos-1).outlinks.size();
            for (auto vertex_matcher : vertices_matcher) {
              auto with_p = with_parts[graph_key_part_mapper->Get(vertex_matcher.first)];
              Vertex* vertex_matcher_vertex = with_p->Find(vertex_matcher.first); 
              for (const Vertex& outlink : vertex_matcher_vertex->outlinks) {
                auto with_p = with_parts[graph_key_part_mapper->Get(outlink.id)];
                Vertex* vertex_to_add = with_p->Find(outlink.id); 

                for (int pos = 0; pos < vertex_pattern.outlinks.size(); pos++) {
                  int next_round_pos = pos + next_round_pos_start;
                  
				  if (vertex_to_add->label == vertex_pattern.outlinks.at(pos).label) {
	      	  	    std::set<int> sibling_ids;
	      	  	    if (vertex_pattern.outlinks.at(pos).siblings.size() == 0) {}
	      	  	    else if (vertex_pattern.outlinks.at(pos).siblings.size() == 1) {
	      	  	      char sibling_label = vertex_pattern.outlinks.at(pos).siblings[0].label;
	      	  		  for (Vertex& v : vertex_matcher_vertex->outlinks) {//TODO
	      	  		    if (v.id == vertex_to_add->id) continue;
	      	  		    if (v.label == sibling_label) {
	      	  		      for (Vertex& v2 : vertex_to_add->outlinks) {
	      	  			    if (v.id == v2.id) {
							  sibling_ids.insert(v.id);
							  break;
							}
	      	  			  }
	      	  		    }
	      	  		  }
	      	  		  if (sibling_ids.empty()) continue;
	      	  	    }
	      	  	    else { CHECK(false); }
                    
					if (matcher.id == 1176038 / num_matchers) {
					  LOG(INFO) << "DEBUG: matcher.matched_round+1: " << matcher.matched_round+1 
						<< " next_round_pos: " << next_round_pos
						<< " vertex_to_add->id: " << vertex_to_add->id 
						<< " vertex_to_add->label: " << vertex_to_add->label
						<< " vertex_matcher.first: " << vertex_matcher.first << "\nsibling_ids: ";
					  for (auto id :sibling_ids) LOG(INFO)<<id;

					}
                    MSG.second.push_back(std::make_tuple(matcher.matched_round+1, next_round_pos,
	        			vertex_to_add->id, vertex_to_add->label, vertex_matcher.first, std::move(sibling_ids)));
                  }
                }
              }
            }
          }
		}
        if (!MSG.second.empty()) kvs.push_back(MSG); // do not send empty msg
	  }
      for (int i = 0; i < num_graph_parts; i++) {
        typed_cache->ReleasePart(i);
      }
      return kvs;
    },
    [iteration, pattern](Matcher* matcher, std::vector<std::tuple<int, int, int, char, int, std::set<int>>> msgs){
	  if (matcher->matched_round == -1) {//first round
		for (auto msg : msgs) {
		  if (std::get<0>(msg) == -1) {//root matching failed
		    CHECK_EQ(std::get<1>(msg), -1);
		    CHECK_NE(std::get<3>(msg), 'a');
		    CHECK_EQ(std::get<4>(msg), -1);
		    CHECK_EQ(std::get<5>(msg).size(), 0);
		  }
		  else {//root matching succeeded
		    CHECK_EQ(std::get<0>(msg), 0);
		    CHECK_EQ(std::get<1>(msg), 0);
		    CHECK_EQ(std::get<3>(msg), 'a');
		    CHECK_EQ(std::get<4>(msg), -1);
		    CHECK_EQ(std::get<5>(msg).size(), 0);
            matcher->result[std::get<0>(msg)][std::get<1>(msg)][std::get<2>(msg)].first = std::get<3>(msg);
		  }
		}
	  }
	  else {//except first round
	    for (auto msg : msgs){
		  CHECK_NE(std::get<0>(msg), -1);
		  CHECK_NE(std::get<0>(msg), 0);
		  for (auto tmp : std::get<5>(msg)) matcher->sibling_result[std::get<2>(msg)].insert(tmp);//TODO
          matcher->result[std::get<0>(msg)][std::get<1>(msg)][std::get<2>(msg)].first = std::get<3>(msg);
          matcher->result[std::get<0>(msg)][std::get<1>(msg)][std::get<2>(msg)].second.push_back(std::get<4>(msg));
		}
	  }
      matcher->matched_round ++;
	  //matcher->UpdateResult(pattern);
    }
  )->SetIter(iteration)
  ->SetStaleness(iteration)
  ->SetName("Main Logic");

 //a-b
 //| |
 // -c-b-d
 auto count = Context::placeholder<CountObj>(1);
  Context::mappartjoin(matcher, count,
    [](TypedPartition<Matcher>* p,
      AbstractMapProgressTracker* t) {
      std::vector<std::pair<int, std::tuple<int64_t, int64_t, int64_t>>> ret;
	  int partition_result = 0;
      for (Matcher& matcher : *p) {
     	int64_t size = 0;
		for (auto tmp1 : matcher.result) {
		  size += 4;
		  for (auto tmp2 : tmp1.second) {
			size += 4;
		    for (auto tmp3 : tmp2.second) {
			  size += 1;
			  size += tmp3.second.second.size()*4;
			}
		  }
		}
		for (auto tmp : matcher.sibling_result) {
		  size += 4;
		  size += tmp.second.size()*4;
		}
        ret.push_back(std::make_pair(0, std::make_tuple(0, size, 0)));

        std::map<int, std::pair<char, std::vector<int>>> pos10 = matcher.result[1][0];
        std::map<int, std::pair<char, std::vector<int>>> pos11 = matcher.result[1][1];
        std::map<int, std::pair<char, std::vector<int>>> pos20 = matcher.result[2][0];
        std::map<int, std::pair<char, std::vector<int>>> pos30 = matcher.result[3][0];
		std::map<int, std::set<int>> sibling_seeds;
		//for (auto& tmp : pos10) {
		//  for (int parent_id : tmp.second.second) {
		//    sibling_seeds[tmp.first].insert(parent_id);
		//  }
		//}
		std::unique_ptr<Executor> exec(new Executor(20));
		std::mutex mu;
		int64_t result = 0;
		for (auto& v : pos30) {
		  CHECK_EQ(v.second.first, 'd');
		  for (int parent_id : v.second.second) {
			int b_id = parent_id;
		    CHECK_EQ(pos20[parent_id].first, 'b');
			exec->Add([b_id, &result, &sibling_seeds,
				&pos20, &pos11, &pos10, &matcher, &mu]() {

  		      size_t local_result = 0;
  			  for (int parent_id : pos20[b_id].second) {
  			    CHECK_EQ(pos11[parent_id].first, 'c');
  			    for (int seed : pos11[parent_id].second) {
  			      for (int sibling : matcher.sibling_result[parent_id]) {
  			  	    if (sibling == b_id) continue;
  			  	    //if (sibling_seeds[sibling].find(seed) != sibling_seeds[sibling].end()) {
  			  	    //  result ++;
  			  	    //}
  			  	    for (int sibling_seed : pos10[sibling].second) {
  			  	      if (sibling_seed == seed) {
  			  	        local_result ++;
  			  	      }
  			  	    }
  			  	  }
  			    }
  			  }
  
  			  std::lock_guard<std::mutex> lk(mu);
  			  result += local_result;

			});
		  }
		}
		exec.reset();
		if (result > 0) {
          ret.push_back(std::make_pair(0, std::make_tuple(result, 0, size)));
		}
		partition_result += result;
        //if (result > 0 && result < 50) {
        //  LOG(INFO) << BLUE("MATCHED NUMBER: ") << result;
		//  LOG(INFO) << matcher.DebugString();
        //}
      }
	  LOG(INFO) << "pid, result: " << p->id << ", " << partition_result;
      return ret;
    },
    [](CountObj* obj, std::tuple<int64_t, int64_t, int64_t> msg){
      obj->count += std::get<0>(msg);
	  obj->total_memory += std::get<1>(msg);
	  obj->matched_memory += std::get<2>(msg);
    }
  )->SetCombine([](std::tuple<int64_t, int64_t, int64_t>* msg1, std::tuple<int64_t, int64_t, int64_t> msg2) {
	std::get<0>(*msg1) += std::get<0>(msg2);
	std::get<1>(*msg1) += std::get<1>(msg2);
	std::get<2>(*msg1) += std::get<2>(msg2);
  })->SetName("Count Matched Pattern");
  Context::mappartjoin(count, count,
    [](TypedPartition<CountObj>* p,
      AbstractMapProgressTracker* t) {
      std::vector<std::pair<int, int>> ret;
      for (CountObj obj : *p) {
	    LOG(INFO) << GREEN("Matched Pattern Number: " + std::to_string(obj.count));
        LOG(INFO) << GREEN("Estimated All Matcher Memory Size: " + std::to_string(obj.total_memory)
			+ " = " + std::to_string(obj.total_memory/1024.0/1024.0/1024.0) + "GB");
        LOG(INFO) << GREEN("Estimated Matched Matcher Memory Size: " + std::to_string(obj.matched_memory)
			+ " = " + std::to_string(obj.matched_memory/1024.0/1024.0/1024.0) + "GB");
	  }
      return ret;
    },
    [](CountObj* obj, int msg) {}
  )->SetName("Print Matched Pattern Number");

  Runner::Run();
}

