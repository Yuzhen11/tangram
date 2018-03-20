#include "core/plan/runner.hpp"

#include "base/color.hpp"

#include <cmath>

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "proj10", "The namenode of hdfs");
DEFINE_int32(hdfs_port, 9000, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

DEFINE_string(url, "", "The url for hdfs file");


DEFINE_int32(kNumPartition, 2, "");
DEFINE_int32(kNumItem, 3, "");
DEFINE_int32(kNumUser, 3, "");
DEFINE_double(eta, 0.1, "");
DEFINE_double(lambda, 0.1, "");
DEFINE_int32(iter, 1, "num of iters");
DEFINE_int32(staleness, 0, "staleness");

using namespace xyz;

const int kNumLatent = 10;

struct Item {
  using KeyT = int;
  KeyT key;
  float latent[kNumLatent];
  void ClearLatent() {
    for (auto& l : latent) {
      l = 0;
    }
  }
  Item() {
    ClearLatent();
  }
  Item(KeyT _key):key(_key) {
    ClearLatent();
  }
  KeyT Key() const { return key; }
  void Init() {
    for (auto& i : latent) {
      i = ((float)rand())/RAND_MAX*2-1;
    }
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << "key: " << key;
    for (auto l : latent) {
      ss << ", " << l;
    }
    return ss.str();
  }
};

using User = Item;
using UserOrItem = Item;

// partitioned by users. 
// indexed by items.
struct DataBlock {
  using KeyT = int;
  KeyT key;  // the partition id
  // item -> {<u, r>, ...}
  std::map<int, std::vector<std::pair<int, float>>> points;

  KeyT Key() const { return key; }
  DataBlock() = default;
  DataBlock(KeyT _key):key(_key) {}
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const DataBlock& d) {
    stream << d.key;  // TODO: no need to serialize the points 
    // as it is only used in distribute and no data at that time.
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, DataBlock& d) {
    stream >> d.key;
    return stream;
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << "key: " << key << ", ";
    for (auto& kv : points) {
      ss << "item: " << kv.first << ": ";
      for (auto& p : kv.second) {
        ss << "<" << p.first << "," << p.second << ">";
      }
      ss << ", ";
    }
    return ss.str();
  }
};

struct Collector {
  using KeyT = int;
  KeyT key;  // the partition id
  // store the user latent factors
  // and the temporary item latent factors 
  std::map<int, User> users;
  std::deque<Item> items;

  Collector() = default;
  Collector(KeyT _key):key(_key) {}
  KeyT Key() const { return key; }
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Collector& c) {
    stream << c.key;  // TODO: no need to serialize users/items.
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Collector& c) {
    stream >> c.key;
    return stream;
  }
};

struct Msg {
  int num_item_processed = -1;  // -1 for item migration, 
                                // others for user updates and a number saying how many items have been processed
  std::map<int, UserOrItem> dict;
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Msg& m) {
    stream << m.num_item_processed << m.dict;
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Msg& m) {
    stream >> m.num_item_processed >> m.dict; 
    return stream;
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << "num_item_processed: " << num_item_processed;
    ss << ", dict size: " << dict.size();
    return ss.str();
  }
};

struct Record {
  int user, item;
  float rating;
};

struct RMSE {
  using KeyT = int;
  RMSE() = default;
  RMSE(KeyT k) : key(k) {}
  KeyT Key() const { return key; }
  KeyT key;
  std::pair<int, float> p;  // count, rmse
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const RMSE& r) {
    stream << r.key << r.p;
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, RMSE& r) {
    stream >> r.key >> r.p;
    return stream;
  }
};

int main(int argc, char** argv) {
  Runner::Init(argc, argv);

  auto load_collection = Context::load(FLAGS_url, [](std::string& s) {
    Record r;
    std::stringstream ss(s);
    std::istream_iterator<std::string> begin(ss);
    std::istream_iterator<std::string> end;
    std::vector<std::string> split(begin, end);
    std::vector<std::string>::iterator it = split.begin();
    r.user = std::stoi(*it++);
    r.item = std::stoi(*it++);
    r.rating = std::stoi(*it++);
    return r;
  }, 1000);

  // distribute the ratings to data_blocks
  auto data_blocks = Context::placeholder<DataBlock, RoundRobinKeyToPartMapper<int>>(FLAGS_kNumPartition);
  Context::mapjoin(load_collection, data_blocks,
    [](const Record& r) {
      return std::make_pair(r.user % FLAGS_kNumPartition, r);
    },
    [](DataBlock* d, const Record& r) {
      d->points[r.item].push_back({r.user, r.rating});
    })->SetName("partition the ratings according to user");

  // distribute the users latent factor and item latent factor (initial distribution)
  auto dummy_distribute_collection = Context::distribute(
          std::vector<int>{0}, 1, "init the collector");
  auto collectors = Context::placeholder<Collector, RoundRobinKeyToPartMapper<int>>(FLAGS_kNumPartition);
  Context::mapjoin(dummy_distribute_collection, collectors,
    [](int) {
      std::vector<std::pair<int, int>> ret;
      for (int i = 0; i < FLAGS_kNumPartition; ++ i) {
        ret.push_back({i, i});
      }
      return ret;
    },
    [](Collector* c, int) {
      for (int i = 0; i < FLAGS_kNumUser; ++ i) {
        if (i % FLAGS_kNumPartition == c->Key()) {
          c->users.insert({i, User(i)});
          c->users[i].Init();
        }
      }
      for (int i = 0; i < FLAGS_kNumItem; ++ i) {
        if (i % FLAGS_kNumPartition == c->Key()) {
          c->items.push_back(Item(i));
          c->items.back().Init();
        }
      }
    })->SetName("init the collector");

  // print info
  /*
  Context::foreach(data_blocks, [](const DataBlock& d) {
    LOG(INFO) << d.DebugString();
  });
  */

  // main logic
  Context::mappartwithjoin(data_blocks, collectors, collectors,
    [](TypedPartition<DataBlock>* p,
       TypedCache<Collector>* typed_cache,
       AbstractMapProgressTracker* t) {
      CHECK_EQ(p->GetSize(), 1);
      auto with_part = typed_cache->GetPartition(p->id);
      CHECK_EQ(with_part->GetSize(), 1);
      auto iter = p->begin();
      auto with_iter = static_cast<TypedPartition<Collector>*>(with_part.get())->begin();

      Msg update_users;
      Msg migrate_items;
      migrate_items.num_item_processed = -1;
      update_users.num_item_processed = 0;
      auto& items = with_iter->items;
      int c = 0;
      int sample = 5;
      for (auto& item : items) {
        // LOG(INFO) << "item: " << item.DebugString();
        update_users.num_item_processed += 1;
        migrate_items.dict[item.key] = item;
        auto& migrate_item = migrate_items.dict[item.key];
        for (auto& u_r: iter->points[item.key]) {
          auto& user = with_iter->users[u_r.first];
          // LOG(INFO) << "user: " << user.DebugString();
          float diff = -std::inner_product(user.latent, user.latent+kNumLatent, item.latent, -u_r.second);
          if (c < sample) {
            LOG(INFO) << "uid, iid: " << user.key << "," << item.key << ", estimated, real: " << std::inner_product(user.latent, user.latent+kNumLatent, item.latent, 0.0) << ", " << u_r.second;
          }
          c += 1;
          auto& user_update = update_users.dict[user.key];
          for (int i = 0; i < kNumLatent; ++ i) {
            migrate_item.latent[i] += FLAGS_eta*(user.latent[i]*diff - FLAGS_lambda*item.latent[i]);
            user_update.latent[i] += FLAGS_eta*(item.latent[i]*diff - FLAGS_lambda*user.latent[i]);
          }
        }
      }
      typed_cache->ReleasePart(p->id);  // donot forget to call this
      std::vector<std::pair<int, Msg>> ret;
      ret.emplace_back(p->id, std::move(update_users));
      ret.emplace_back((p->id+1)%FLAGS_kNumPartition, std::move(migrate_items));
      // each map send out two Msgs, one to the collector in this part and the other to the collector in next part.
      // e.g. id = 0, send update_users to 0, and send migrate_items to 1.
      // the system should make sure local update will apply before next map 
      // (this will be ensured by local_map_mode, as in local_map_mode, the local message will
      // send to local controller queue before the finish map signal).
      return ret;
    },
    [](Collector* collector, const Msg& msg) {
      // LOG(INFO) << msg.DebugString();
      if (msg.num_item_processed == -1) {  // migrate item
        for (auto& kv : msg.dict) {
          collector->items.push_back(kv.second);
        }
      } else {  // update users
        for (auto& kv : msg.dict) {
          auto& user = collector->users[kv.first].latent;
          auto& update = kv.second.latent;
          for (int i = 0; i < kNumLatent; ++ i) {
            user[i] += update[i];
          }
        }
        // remove the first num_item_processed.
        for (int i = 0; i < msg.num_item_processed; ++ i) {
          collector->items.pop_front();
        }
      }
    })->SetName("main loop")->SetIter(FLAGS_iter)->SetStaleness(FLAGS_staleness);

  // rmse
  // this mse only calculate a part of the training samples!
  auto rmse = Context::placeholder<RMSE>(1);
  Context::mappartwithjoin(data_blocks, collectors, rmse,
    [](TypedPartition<DataBlock>* p,
       TypedCache<Collector>* typed_cache,
       AbstractMapProgressTracker* t) {
      CHECK_EQ(p->GetSize(), 1);
      auto with_part = typed_cache->GetPartition(p->id);
      CHECK_EQ(with_part->GetSize(), 1);
      auto iter = p->begin();
      auto with_iter = static_cast<TypedPartition<Collector>*>(with_part.get())->begin();

      int count = 0;
      float local_rmse = 0;
      auto& items = with_iter->items;
      for (auto& item : items) {
        for (auto& u_r: iter->points[item.key]) {
          auto& user = with_iter->users[u_r.first];
          float r = std::inner_product(user.latent, user.latent+kNumLatent, item.latent, 0.0);
          local_rmse += pow(fabs(r-u_r.second), 2);
          count += 1;
        }
      }
      typed_cache->ReleasePart(p->id);  // donot forget to call this
      std::vector<std::pair<int, std::pair<int, float>>> ret;
      ret.emplace_back(0, std::make_pair(count, local_rmse));
      return ret;
    },
    [](RMSE* rmse, std::pair<int, float> m) {
      // LOG(INFO) << "join rmse: " << m.first << " " << m.second;
      rmse->p.first += m.first;
      rmse->p.second += m.second;
    })->SetName("rmse");
  Context::foreach(rmse, [](RMSE r) {
    CHECK_GT(r.p.first, 0);
    std::stringstream ss;
    ss << "num samples: " << r.p.first << " mse: " << r.p.second/r.p.first;
    LOG(INFO) << BLUE(ss.str());
  });

  Runner::Run();
  // Runner::PrintDag();
}
