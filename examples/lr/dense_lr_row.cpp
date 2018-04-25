#include "examples/lr/basic_lr.hpp"

#include "core/index/range_key_to_part_mapper.hpp"

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
  float alpha = FLAGS_alpha;
  const int num_param_per_part = FLAGS_num_param_per_part;

  std::vector<third_party::Range> ranges;
  int num_param_parts = num_params / num_param_per_part;
  if (num_params % num_param_per_part != 0) {
    num_param_parts += 1;
  }
  // there are num_param_parts elements and num_param_parts partition.
  // the partition number does not need to be num_param_parts.

  // init the param
  auto dense_rows =
      create_dense_rows(num_param_parts, num_params, num_param_per_part);

#ifdef ENABLE_CP
  Context::checkpoint(params, "/tmp/tmp/yz");
  Context::checkpoint(points, "/tmp/tmp/yz");
#endif

  auto p1 =
      Context::mappartwithjoin(
          points, dense_rows, dense_rows,
          [num_params, alpha,
           num_param_parts, num_param_per_part](TypedPartition<IndexedPoints> *p, TypedCache<DenseRow> *typed_cache,
                      AbstractMapProgressTracker *t) {
            int correct_count = 0;

            // 0. init params (allocate)
            auto begin_time = std::chrono::steady_clock::now();
            std::vector<float> step_sum(num_params, 0);
            auto end_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time);
            LOG_IF(INFO, p->id == 0) << GREEN("Parameter init time: " + 
                          std::to_string(duration.count())
                          + "ms on part 0");

            // 1. prepare params
            begin_time = std::chrono::steady_clock::now();
            auto with_parts = prepare_lr_params<DenseRow>(num_param_parts, typed_cache);

            // TOOD:FT for fetching map is not supported yet!
            // so make sure to kill after fetching
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("sleeping");
            // std::this_thread::sleep_for(std::chrono::seconds(1));

            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time);
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("Parameter prepare time: " + std::to_string(duration.count()));
            LOG_IF(INFO, p->id == 0) << GREEN("Parameter prepare time: " + 
                          std::to_string(duration.count())
                          + "ms on part 0");

            // 2. copy params

            begin_time = std::chrono::steady_clock::now();
            auto old_params = copy_lr_params(with_parts, num_param_parts,
                                             num_params, num_param_per_part);

            for (int i = 0; i < num_param_parts; i++) {
              typed_cache->ReleasePart(i);
            }

            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time);
            LOG_IF(INFO, p->id == 0) << GREEN("Parameter copy time: " + 
                          std::to_string(duration.count())
                          + "ms on part 0");

            // 3. calculate
            begin_time = std::chrono::steady_clock::now();
            int count = 0;

            for (int replicate = 0; replicate < FLAGS_replicate_factor; ++ replicate) {

            auto data_iter = p->begin();
            auto end_iter = p->end();
            while (data_iter != end_iter) {
              for (auto& point : data_iter->points) {
                auto &x = point.x;
                auto y = point.y;
                if (y < 0)
                  y = 0;

                float pred_y = 0.0;
                for (auto& field : x) {
                    pred_y += old_params[field.first] * field.second;
                }
                pred_y += old_params[num_params - 1]; // intercept
                pred_y = 1. / (1. + exp(-1 * pred_y));

                if ((y == 0 && pred_y < 0.5) || (y == 1 && pred_y >= 0.5)) {
                    correct_count++;
                }
                float diff = alpha * (y - pred_y);
                for (auto& field : x) {
                    step_sum[field.first] += diff * field.second;
                }
                step_sum[num_params - 1] += diff; // intercept
                count++;
              }
              ++data_iter;
            }

            }  // replicate

            // create the output
            auto kvs = create_lr_output(num_param_parts, num_param_per_part,
                                        num_params, count, step_sum);
            
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

            return kvs;
          },
          [](DenseRow* row, std::vector<float> v) { 
            CHECK_EQ(row->params.size(), v.size());
            for (int i = 0; i < v.size(); ++ i) {
              row->params[i] += v[i];
            }
          })
          ->SetIter(FLAGS_num_iter)
          ->SetStaleness(FLAGS_staleness)
#ifdef ENABLE_CP
          ->SetCheckpointInterval(5, "/tmp/tmp/yz")
#endif
          ->SetCombine([](std::vector<float>* v, const std::vector<float>& o) {
            CHECK_EQ(v->size(), o.size());
            for (int i = 0; i < v->size(); ++ i) {
              (*v)[i] += o[i];
            }
          }, combine_timeout);

  // Context::count(params);
  // Context::count(points);
  Runner::Run();
}
