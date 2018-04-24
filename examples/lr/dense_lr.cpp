#include "examples/lr/basic_lr.hpp"

// #define ENABLE_CP

int main(int argc, char **argv) {
  Runner::Init(argc, argv);
  const int combine_timeout = ParseCombineTimeout(FLAGS_combine_type);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "combine_type: " << FLAGS_combine_type << ", timeout: " << combine_timeout;
  }

  // load and generate two collections
  auto dataset = load_data();

  // Repartition the data
  auto points = repartition(dataset);

  int num_params = FLAGS_num_params + 2;
  double alpha = FLAGS_alpha;
  const int num_param_per_part = FLAGS_num_param_per_part;

  int num_param_parts = num_params / num_param_per_part;
  if (num_params % num_param_per_part != 0) {
    num_param_parts += 1;
  }
  auto params = create_range_params(num_params, num_param_per_part);

#ifdef ENABLE_CP
  Context::checkpoint(params, "/tmp/tmp/yz");
  Context::checkpoint(points, "/tmp/tmp/yz");
#endif

  auto p1 =
      Context::mappartwithjoin(
          points, params, params,
          [num_params, alpha,
           num_param_parts](TypedPartition<IndexedPoints> *p, TypedCache<Param> *typed_cache,
                      AbstractMapProgressTracker *t) {
            std::vector<std::pair<int, float>> kvs(num_params);
            std::vector<float> step_sum(num_params, 0);
            int correct_count = 0;

            // 1. prepare params
            auto begin_time = std::chrono::steady_clock::now();
            std::vector<std::shared_ptr<TypedPartition<Param>>> with_parts(num_param_parts);
            int start_idx =
                rand() %
                num_param_parts; // random start_idx to avoid overload on one point
            for (int i = 0; i < num_param_parts; i++) {
              int idx = (start_idx + i) % num_param_parts;
              auto part = typed_cache->GetPartition(idx);
              with_parts[idx] = std::dynamic_pointer_cast<TypedPartition<Param>>(part);
            }

            // TOOD:FT for fetching map is not supported yet!
            // so make sure to kill after fetching
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("sleeping");
            // std::this_thread::sleep_for(std::chrono::seconds(1));

            auto end_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time);
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("Parameter prepare time: " + std::to_string(duration.count()));
            LOG_IF(INFO, p->id == 0) << GREEN("Parameter prepare time: " + 
                          std::to_string(duration.count())
                          + "ms on part 0");

            // 2. copy params
            begin_time = std::chrono::steady_clock::now();
            std::vector<float> old_params(num_params);
            for (auto with_p : with_parts) {
              auto iter1 = with_p->begin();
              auto end_iter = with_p->end();
              while (iter1 != end_iter) {
                CHECK_LT(iter1->fea, num_params);
                old_params[iter1->fea] = iter1->val;
                ++iter1;
              }
            }

            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time);
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("Parameter copy time: " + std::to_string(duration.count()));
            LOG_IF(INFO, p->id == 0) << GREEN("Parameter copy time: " + 
                          std::to_string(duration.count())
                          + "ms on part 0");

            // 3. calculate
            begin_time = std::chrono::steady_clock::now();
            auto data_iter = p->begin();
            int count = 0;
            auto end_iter = p->end();
            while (data_iter != end_iter) {
              for (auto& point : data_iter->points) {
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
              ++data_iter;
            }

            for (int i = 0; i < num_params; i++) {
              step_sum[i] /= count;
              kvs[i] = std::make_pair(i, step_sum[i]);
            }
            
            LOG_IF(INFO, p->id == 0) << RED("Correct: " + std::to_string(correct_count) +
                             ", Batch size: " + std::to_string(count) +
                             ", Accuracy: " +
                             std::to_string(correct_count / float(count))
                          + " on part 0");

            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time);
            LOG_IF(INFO, p->id == 0) << GREEN("Computation time: " + 
                          std::to_string(duration.count())
                          + "ms on part 0");

            for (int i = 0; i < num_param_parts; i++) {
              typed_cache->ReleasePart(i);
            }
               
            return kvs;
          },
          [](Param *param, float val) { param->val += val; })
          ->SetIter(FLAGS_num_iter)
          ->SetStaleness(FLAGS_staleness)
#ifdef ENABLE_CP
          ->SetCheckpointInterval(5, "/tmp/tmp/yz")
#endif
          ->SetCombine([](float *a, float b) { *a = *a + b; }, combine_timeout);

  // Context::count(params);
  // Context::count(points);
  Runner::Run();
}
