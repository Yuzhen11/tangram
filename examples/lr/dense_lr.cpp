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

  int num_params = FLAGS_num_params;
  double alpha = FLAGS_alpha;
  int num_parts = FLAGS_num_parts;
  int batch_size = FLAGS_batch_size;

  std::vector<third_party::Range> ranges;
  CHECK_GT(num_parts, 0);
  if (num_params % num_parts != 0) {
    int params_per_part = num_params/num_parts+1;
    for (int i = 0; i < num_parts-1; ++ i) {
      ranges.push_back(third_party::Range(i*params_per_part, (i+1)*params_per_part));
    }
    ranges.push_back(third_party::Range((num_parts-1)*params_per_part, num_params));
  } else {
    int params_per_part = num_params/num_parts;
    for (int i = 0; i < num_parts; ++ i) {
      ranges.push_back(third_party::Range(i*params_per_part, (i+1)*params_per_part));
    }
  }
  CHECK_EQ(ranges.size(), num_parts);
  auto range_key_to_part_mapper = std::make_shared<RangeKeyToPartMapper<int>>(ranges);
  auto params = Context::range_placeholder<Param>(range_key_to_part_mapper);
  auto p =
      Context::mappartwithjoin(
          dataset, params, params,
          [num_params, alpha, batch_size,
           num_parts](TypedPartition<Point> *p, TypedCache<Param> *typed_cache,
                      AbstractMapProgressTracker *t) {
            std::vector<std::pair<int, float>> kvs;
            std::vector<float> step_sum(num_params, 0);
            int correct_count = 0;
            int num_data = 0;
            int batch_size_ = batch_size;

            auto iter0 = p->begin();
            while (iter0 != p->end()) {
              num_data++;
              ++iter0;
            }

            // mini-batch starting position
            int offset = 0;
            if (FLAGS_is_sgd) {
              CHECK_GT(num_data, batch_size);
              offset = rand() % (num_data - batch_size);
            } else
              batch_size_ = num_data;

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
            for (int i = 0; i < offset; i++)
              ++iter2;
            for (int i = 0; i < batch_size_; i++) {
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

            for (int i = 0; i < num_params; i++) {
              step_sum[i] /= num_params;
            }
            for (int j = 0; j < num_params; j++) {
              kvs.push_back({j, step_sum[j]});
            }
            /*
            LOG(INFO) << RED("Correct: " + std::to_string(correct_count) +
                             ", Batch size: " + std::to_string(batch_size_) +
                             ", Accuracy: " +
                             std::to_string(correct_count /
               float(batch_size_)));
               */
            return kvs;
          },
          [](Param *param, float val) { param->val += val; })
          ->SetIter(FLAGS_num_iter)
          ->SetStaleness(FLAGS_staleness)
          ->SetCombine([](float *a, float b) { *a = *a + b; });

  Context::count(params);
  Context::count(dataset);
  Runner::Run();
}
