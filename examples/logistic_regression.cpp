#include "base/color.hpp"
#include "core/plan/runner.hpp"
#include "boost/tokenizer.hpp"

#include <string>
#include <cmath>

DEFINE_string(url, "", "The url for hdfs file");
DEFINE_int32(num_params, -1, "The number of parameters in the dataset");
DEFINE_int32(num_data, -1, "The number of data in the dataset");
DEFINE_double(alpha, 0.1, "The learning rate of the model");
DEFINE_int32(num_iter, 1, "The number of iterations");
DEFINE_int32(staleness, 0, "Staleness for the SSP");
DEFINE_bool(is_sparse, false, "Is the dataset sparse or not");

using namespace xyz;

struct Point {
  Point() = default;
  // <Fea, Val>
  std::vector<std::pair<int, float>> x;
  // Label
  int y;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Point &point) {
    stream << point.y << point.x;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Point &point) {
    stream >> point.y >> point.x;
    return stream;
  }
};

struct Param {
  using KeyT = int;
  Param() = default;
  Param(KeyT _fea) : fea(_fea) {}
  KeyT Key() const { return fea; }
  KeyT fea;
  float val = 0;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Param &param) {
    stream << param.fea << param.val;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Param &param) {
    stream >> param.fea >> param.val;
    return stream;
  }
};

int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  // load and generate two collections
  auto dataset = Context::load(FLAGS_url, [](std::string s) {
    Point point;
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();

    point.y = std::stoi(*it);
    it ++;
    for (; it != tok.end(); ++it) {
      std::vector<std::string> fea_val;
      std::istringstream ss2(*it);
      std::string token;
      while (std::getline(ss2, token, ':')) {
        fea_val.push_back(token);
      }
      CHECK_EQ(fea_val.size(), 2);
      int fea = std::stoi(fea_val[0]);
      float val = std::stof(fea_val[1]);
      point.x.push_back(std::make_pair(fea, val));
    }

    return point;
  });

  int num_params = FLAGS_num_params;
  int num_data = FLAGS_num_data;
  double alpha = FLAGS_alpha;
  int num_parts = 5;

  auto params = Context::placeholder<Param>(num_parts);
  auto p =
      Context::mappartwithjoin(
          dataset, params, params,
          [num_params, num_data, alpha, num_parts](TypedPartition<Point> *p,
                                        TypedCache<Param> *typed_cache,
                                        AbstractMapProgressTracker *t) {
            std::vector<std::pair<int, float>> kvs;
            std::vector<float> step_sum(num_params, 0);
            int correct_count = 0;

            // Dense
            if (!FLAGS_is_sparse) {
              // auto part = typed_cache->GetPartition(0);
              // auto *with_p = static_cast<TypedPartition<Param> *>(part.get());
              // LOG(INFO) << GREEN(std::to_string(p->GetSize()));
              // LOG(INFO) << GREEN(std::to_string(with_p->GetSize()));

              std::vector<TypedPartition<Param>*> with_parts;
              for (int i = 0; i < num_parts; i++) {
                auto part = typed_cache->GetPartition(i);
                auto *with_p = static_cast<TypedPartition<Param>*>(part.get());
                with_parts.push_back(with_p);
              }

              std::vector<float> old_params(num_params);
              for (auto with_p : with_parts) {
                  auto iter1 = with_p->begin();
                  while (iter1 != with_p->end()) {
                      CHECK_LT(iter1->fea, num_params);
                      old_params[iter1->fea] = iter1->val;
                      ++iter1;
                  }
              }

              auto iter2 = p->begin();
              while (iter2 != p->end()) {
                auto &x = iter2->x;
                auto y = iter2->y;
                if (y < 0)
                  y = 0;

                float pred_y = 0.0;
                for (auto field : x) {
                  pred_y += old_params[field.first] * field.second;
                }
                pred_y += old_params[num_params - 1]; // intercept
                pred_y = 1. / (1. + exp(-1 * pred_y));

                if ((y == 0 && pred_y < 0.5) || (y == 1 && pred_y >= 0.5)) {
                  correct_count++;
                }
                for (auto field : x) {
                  step_sum[field.first] += alpha * field.second * (y - pred_y);
                }
                step_sum[num_params - 1] += alpha * (y - pred_y); // intercept
                ++iter2;
              }

              for (int i = 0; i < num_parts; i++) {
                typed_cache->ReleasePart(i);
              }
            }

            // Sparse
            else {
              std::set<int> keys_set;
              std::map<int, float> old_params;
              auto iter = p->begin();
              while (iter != p->end()) {
                auto &x = iter->x;
                for (auto field : x) {
                  keys_set.insert(field.first);
                }
                ++iter;
              }
              keys_set.insert(num_params - 1);
              std::vector<int> keys(keys_set.begin(), keys_set.end());
              auto objs = typed_cache->Get(keys);
              CHECK_EQ(keys.size(), objs.size());
              for (int i = 0; i < keys.size(); i++)
                old_params.insert(std::make_pair(objs[i].fea, objs[i].val));

              auto iter2 = p->begin();
              while (iter2 != p->end()) {
                auto &x = iter2->x;
                auto y = iter2->y;
                if (y < 0)
                  y = 0;

                float pred_y = 0.0;
                for (auto field : x) {
                  pred_y += old_params[field.first] * field.second;
                }
                pred_y += old_params[num_params - 1]; // intercept
                pred_y = 1. / (1. + exp(-1 * pred_y));

                if ((y == 0 && pred_y < 0.5) || (y == 1 && pred_y >= 0.5)) {
                  correct_count++;
                }
                for (auto field : x) {
                  step_sum[field.first] += alpha * field.second * (y - pred_y);
                }
                step_sum[num_params - 1] += alpha * (y - pred_y); // intercept
                ++iter2;
              }
            }
            
            for (int i = 0; i < num_params; i++) {
              step_sum[i] /= num_params;
            }
            for (int j = 0; j < num_params; j++) {
              kvs.push_back({j, step_sum[j]});
            }
            LOG(INFO) << RED("Correct: " + std::to_string(correct_count) +
                             ", Count: " + std::to_string(num_data) +
                             ", Accuracy: " +
                             std::to_string(correct_count / float(num_data)));
            return kvs;
          },
          [](Param *param, float val) { param->val += val; })
          ->SetIter(FLAGS_num_iter)
          ->SetStaleness(FLAGS_staleness)
          ->SetCombine([](float* a, float b) { *a = *a + b; });

  Context::count(params);
  Context::count(dataset);
  Runner::Run();
}
