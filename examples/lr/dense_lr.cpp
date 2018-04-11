#include "examples/lr/basic_lr.hpp"

#include "core/index/range_key_to_part_mapper.hpp"

#include <ctime>

int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  // load and generate two collections
  const clock_t begin_time = clock();
  auto dataset = load_data();
  LOG(INFO) << GREEN("Data loading time: " + 
      std::to_string(float(clock() - begin_time) / CLOCKS_PER_SEC));

  // Repartition the data
  auto points = Context::placeholder<IndexedPoints>(20)->SetName("points");
  auto p0 = Context::mappartjoin(dataset, points,
    [](TypedPartition<Point>* p,
      AbstractMapProgressTracker* t) {
      std::vector<std::pair<int,Point>> all;
      for (auto& v: *p) {
        all.push_back(std::make_pair(rand() % 20, v));
      }
      return all;
    },
    [](IndexedPoints* ip, Point p) {
      ip->points.push_back(p);
    })
    ->SetName("construct points from dataset");


  int num_params = FLAGS_num_params + 2;
  double alpha = FLAGS_alpha;
  int num_parts = FLAGS_num_parts;
  bool is_sgd = FLAGS_is_sgd;

  std::vector<third_party::Range> ranges;
  CHECK_GT(num_parts, 0);
  if (num_params % num_parts != 0) {
    int params_per_part = num_params / num_parts + 1;
    for (int i = 0; i < num_parts - 1; ++ i) {
      ranges.push_back(third_party::Range(i * params_per_part, (i+1) * params_per_part));
    }
    ranges.push_back(third_party::Range((num_parts-1)*params_per_part, num_params));
  } else {
    int params_per_part = num_params / num_parts;
    for (int i = 0; i < num_parts; ++ i) {
      ranges.push_back(third_party::Range(i * params_per_part, (i+1) * params_per_part));
    }
  }
  CHECK_EQ(ranges.size(), num_parts);
  auto range_key_to_part_mapper = std::make_shared<RangeKeyToPartMapper<int>>(ranges);
  auto params = Context::range_placeholder<Param>(range_key_to_part_mapper);
  auto p1 =
      Context::mappartwithjoin(
          points, params, params,
          [num_params, alpha, is_sgd,
           num_parts](TypedPartition<IndexedPoints> *p, TypedCache<Param> *typed_cache,
                      AbstractMapProgressTracker *t) {
            std::vector<std::pair<int, float>> kvs;
            std::vector<float> step_sum(num_params, 0);
            int correct_count = 0;

            std::vector<TypedPartition<Param> *> with_parts(num_parts);
            int start_idx =
                rand() %
                num_parts; // random start_idx to avoid overload on one point
            for (int i = 0; i < num_parts; i++) {
              int idx = (start_idx + i) % num_parts;
              auto part = typed_cache->GetPartition(idx);
              auto *with_p = static_cast<TypedPartition<Param> *>(part.get());
              with_parts[idx] = with_p;
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
            int count = 0;
            int sgd_counter = -1;
            while (iter2 != p->end()) {
              for (auto point : iter2->points) {
                // sgd: pick 1 point out of 10
                if (is_sgd) {
                  sgd_counter++;
                  if (sgd_counter % 10 != 0)
                    continue;
                }

                auto &x = point.x;
                auto y = point.y;
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
                count++;
              }
              ++iter2;
            }

            for (int i = 0; i < num_parts; i++) {
              typed_cache->ReleasePart(i);
            }

            for (int i = 0; i < num_params; i++) {
              step_sum[i] /= num_params;
            }
            for (int j = 0; j < num_params; j++) {
              kvs.push_back({j, step_sum[j]});
            }
            
            LOG(INFO) << RED("Correct: " + std::to_string(correct_count) +
                             ", Batch size: " + std::to_string(count) +
                             ", Accuracy: " +
                             std::to_string(correct_count /
               float(count)));
               
            return kvs;
          },
          [](Param *param, float val) { param->val += val; })
          ->SetIter(FLAGS_num_iter)
          ->SetStaleness(FLAGS_staleness)
          ->SetCombine([](float *a, float b) { *a = *a + b; });

  Context::count(params);
  Context::count(points);
  Runner::Run();
}
