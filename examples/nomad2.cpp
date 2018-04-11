#include "core/plan/runner.hpp"

#include "base/color.hpp"

#include <boost/tokenizer.hpp>

#include <cmath>

DEFINE_string(url, "", "The url for hdfs file");

DEFINE_int32(kNumPartition, 2, "");
DEFINE_int32(kNumItem, 3, "");
DEFINE_int32(kNumUser, 3, "");
// DEFINE_double(eta, 0.1, "");
DEFINE_double(lambda, 0.1, "");
DEFINE_int32(iter, 1, "num of iters");
DEFINE_int32(staleness, 0, "staleness");
DEFINE_int32(num_line_per_part, -1, "num_line_per_part");
DEFINE_int32(backoff_time, 100, "backoff time if there is no item in ms");
DEFINE_int32(max_sample_item_size_each_round, -1, "");
DEFINE_int32(max_retry, 0, "may set to 0 for bsp");

DEFINE_double(alpha, -1, "");
DEFINE_double(beta, -1, "");
// for yahoo music
// alpha = 0.00075;
// beta = 0.01;
// lambda = 1;
//
// for netflix
// alpha = 0.012;
// beta = 0.05;
// lambda = 0.05;

using namespace xyz;

const int kNumLatent = 100;

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
      // i = ((float)rand())/RAND_MAX*2-1;
      i = ((float)rand())/RAND_MAX* sqrt(5.)/sqrt(kNumLatent*1.0);
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
  // item -> {<u, r, update_count>, ...}
  std::map<int, std::vector<std::tuple<int, float, int>>> points;

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
        ss << "<" << std::get<0>(p) << "," << std::get<1>(p) << "," << std::get<2>(p) << ">";
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
  int version;

  Collector() = default;
  Collector(KeyT _key):key(_key) {}
  KeyT Key() const { return key; }
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Collector& c) {
    stream << c.key << c.users << c.items << c.version;  // TODO
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Collector& c) {
    stream >> c.key >> c.users >> c.items >> c.version;
    return stream;
  }
};

struct Msg {
  int num_item_processed = -1;  // -1 for item migration, 
                                // others for user updates and a number saying how many items have been processed
  std::map<int, UserOrItem> dict;
  int version;  // debug
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Msg& m) {
    stream << m.num_item_processed << m.dict << m.version;
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Msg& m) {
    stream >> m.num_item_processed >> m.dict >> m.version; 
    return stream;
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << "num_item_processed: " << num_item_processed;
    ss << ", dict size: " << dict.size();
    ss << ", version: " << version;
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

  CHECK_NE(FLAGS_alpha, double(-1));
  CHECK_NE(FLAGS_beta, double(-1));

  auto load_collection = Context::load(FLAGS_url, [](std::string s) {
    Record r;
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    auto it = tok.begin();
    r.user = std::stoi(*it++);
    r.item = std::stoi(*it++);
    r.rating = std::stof(*it++);
    return r;
  }, FLAGS_num_line_per_part);

  // distribute the ratings to data_blocks
  auto data_blocks = Context::placeholder<DataBlock, RoundRobinKeyToPartMapper<int>>(FLAGS_kNumPartition);
  Context::mapjoin(load_collection, data_blocks,
    [](const Record& r) {
      return std::make_pair(r.user % FLAGS_kNumPartition, r);
    },
    [](DataBlock* d, const Record& r) {
      d->points[r.item].push_back(std::make_tuple(r.user, r.rating, 0));
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
      int version = typed_cache->GetVersion();

      // FLAGS_max_retry may set to 0 for bsp
      int retry = 0;
      while (with_iter->items.empty() && FLAGS_max_retry) {
        // LOG(INFO) << "retrying " << p->id << " no items, sleep for " << FLAGS_backoff_time << " ms";
        typed_cache->ReleasePart(p->id);
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_backoff_time));
        with_part = typed_cache->GetPartition(p->id);
        with_iter = static_cast<TypedPartition<Collector>*>(with_part.get())->begin();
        retry += 1;
        if (retry == FLAGS_max_retry) {
          // LOG(INFO) << p->id << " reach max retry";
          break;
        }
      }
      // LOG(INFO) << "item count: " << items.size() << " on " << p->id;
      if (with_iter->items.empty()) {
        LOG(INFO) << p->id << " no items, sleep for " << FLAGS_backoff_time << " ms";
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_backoff_time));
      }

      // Msg update_users;
      // Msg migrate_items;
      std::vector<std::pair<int, Msg>> msgs(FLAGS_kNumPartition+1);
      for (int i = 0; i < FLAGS_kNumPartition; ++ i) {
        msgs[i].first = i;
        msgs[i].second.num_item_processed = -1;
      }
      msgs[FLAGS_kNumPartition].first = p->id;  // the update_users
      Msg& update_users = msgs[FLAGS_kNumPartition].second;
      update_users.version = version;
      update_users.num_item_processed = 0;

      // migrate_items.num_item_processed = -1;
      auto& items = with_iter->items;
      if (version > 0)
        CHECK_EQ(version, with_iter->version + 1);
      int c = 0;
      int sample = 2;
      int item_count = 0;
      for (auto& item : items) {
        item_count += 1;
        if (item_count == FLAGS_max_sample_item_size_each_round) {
          LOG(INFO) << p->id << " truncated by max_sample_item_size_each_round: " << FLAGS_max_sample_item_size_each_round;
          break;
        }
        // choose a migrate item
        int k = rand()%FLAGS_kNumPartition;
        auto& migrate_items = msgs[k].second;
        // LOG(INFO) << "item: " << item.DebugString();
        update_users.num_item_processed += 1;
        migrate_items.dict[item.key] = item;
        auto& migrate_item = migrate_items.dict[item.key];
        for (auto& u_r_c: iter->points[item.key]) {
          auto& user = with_iter->users[std::get<0>(u_r_c)];
          int t = std::get<2>(u_r_c);
          // LOG(INFO) << "t: " << t;
          float lr = FLAGS_alpha / (1 + FLAGS_beta*pow(t, 1.5));
          float diff = - std::inner_product(user.latent, user.latent+kNumLatent, migrate_item.latent, - std::get<1>(u_r_c));
          if (c < sample && p->id == 0) {
            // LOG(INFO) << "user: " << user.DebugString();
            LOG(INFO) << "uid, iid: " << user.key << "," << item.key << ", estimated, real: " 
              << std::inner_product(user.latent, user.latent+kNumLatent, migrate_item.latent, 0.0) << ", " << std::get<1>(u_r_c)
              << ", lr: " << lr << ", t: " << t;
          }
          c += 1;
          std::get<2>(u_r_c) += 1;  // fuck! update map element
          auto& user_update = update_users.dict[user.key];
          for (int i = 0; i < kNumLatent; ++ i) {
            migrate_item.latent[i] += lr*(user.latent[i]*diff - FLAGS_lambda*migrate_item.latent[i]);
            // user_update.latent[i] += lr*(migrate_item.latent[i]*diff - FLAGS_lambda*user.latent[i]);
          }
          diff = - std::inner_product(user.latent, user.latent+kNumLatent, migrate_item.latent, - std::get<1>(u_r_c));
          for (int i = 0; i < kNumLatent; ++ i) {
            user_update.latent[i] += lr*(migrate_item.latent[i]*diff - FLAGS_lambda*user.latent[i]);
          }
          for (int i = 0; i < kNumLatent; ++ i) {
            user.latent[i] += user_update.latent[i];
            user_update.latent[i] = 0;
          }
        }
      }
      typed_cache->ReleasePart(p->id);  // donot forget to call this
      // std::vector<std::pair<int, Msg>> ret;
      // ret.emplace_back(p->id, std::move(update_users));
      // ret.emplace_back((p->id+1)%FLAGS_kNumPartition, std::move(migrate_items));
      // each map send out two Msgs, one to the collector in this part and the other to the collector in next part.
      // e.g. id = 0, send update_users to 0, and send migrate_items to 1.
      // the system should make sure local update will apply before next map 
      // (this will be ensured by local_map_mode, as in local_map_mode, the local message will
      // send to local controller queue before the finish map signal).
      return msgs;
    },
    [](Collector* collector, const Msg& msg) {
      // LOG(INFO) << msg.DebugString();
      if (msg.num_item_processed == -1) {  // migrate item
        for (auto& kv : msg.dict) {
          collector->items.push_back(kv.second);
        }
      } else {  // update users
        collector->version = msg.version;
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

      // LOG(INFO) << "part id, fetch id: " << p->id;
      auto with_part = typed_cache->GetPartition(p->id);
      CHECK_EQ(with_part->GetSize(), 1);
      auto iter = p->begin();
      auto my_with_iter = static_cast<TypedPartition<Collector>*>(with_part.get())->begin();

      int count = 0;
      float local_rmse = 0;
      auto& items = my_with_iter->items;
      for (auto& item : items) {
        for (auto& u_r_c: iter->points[item.key]) {
          auto& user = my_with_iter->users[std::get<0>(u_r_c)];
          float r = std::inner_product(user.latent, user.latent+kNumLatent, item.latent, 0.0);
          local_rmse += pow(fabs(r-std::get<1>(u_r_c)), 2);
          count += 1;
        }
      }
      // LOG(INFO) << "[Debug] local item size: " << items.size();

      /*
      for (int i = 0; i < FLAGS_kNumPartition; ++ i) {
        if (i == p->id) {
          continue;
        }
        auto with_part = typed_cache->GetPartition(i);
        // LOG(INFO) << "part id, fetch id: " << p->id << ", " << i 
        //   << ", size: " << with_part->GetSize();
        CHECK_EQ(with_part->GetSize(), 1);
        auto iter = p->begin();
        auto with_iter = static_cast<TypedPartition<Collector>*>(with_part.get())->begin();

        auto& items = with_iter->items;
        // LOG(INFO) << "[Debug] part id: " << i << ", local item size: " << items.size();
        for (auto& item : items) {
          for (auto& u_r_c: iter->points[item.key]) {
            auto& user = my_with_iter->users[std::get<0>(u_r_c)];
            float r = std::inner_product(user.latent, user.latent+kNumLatent, item.latent, 0.0);
            local_rmse += pow(fabs(r-std::get<1>(u_r_c)), 2);
            count += 1;
          }
        }
        typed_cache->ReleasePart(i);  // donot forget to call this
        // LOG(INFO) << "part id, fetch id: " << p->id << ", " << i << ", done";
      }
      */

      typed_cache->ReleasePart(p->id);  // donot forget to call this
      // LOG(INFO) << "part id, fetch id: " << p->id << ", done";
      
      // LOG(INFO) << "[Debug] local count, rmse: " << count << ", " << local_rmse;
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
