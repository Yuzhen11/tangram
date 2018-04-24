#include "examples/lr/basic_lr.hpp"

#include "core/index/range_key_to_part_mapper.hpp"

/*
 * Store the param in DenseRow, similar to Bosen.
 */
// #define ENABLE_CP

struct DenseRow {
  using KeyT = int;
  DenseRow() = default;
  DenseRow(KeyT id) : row_id(id) {}
  KeyT Key() const { return row_id; }

  int row_id;
  std::vector<float> params;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const DenseRow& row) {
    stream << row.row_id << row.params;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     DenseRow& row) {
    stream >> row.row_id >> row.params;
    return stream;
  }
};

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

  std::vector<third_party::Range> ranges;
  int num_param_parts = num_params / num_param_per_part;
  if (num_params % num_param_per_part != 0) {
    num_param_parts += 1;
  }
  // there are num_param_parts elements and num_param_parts partition.
  // the partition number does not need to be num_param_parts.
  auto dense_rows = Context::placeholder<DenseRow>(num_param_parts);

  // init the param
  auto dummy_collection = Context::distribute<int>({1});
  Context::mapjoin(dummy_collection, dense_rows, 
      [num_param_parts](int) {
        std::vector<std::pair<int, int>> ret;
        for (int i = 0;  i < num_param_parts; ++ i) {
          ret.push_back({i, 0});
        }
        return ret;
      },
      [num_param_parts, num_params, num_param_per_part](DenseRow* row, int) {
        if ((row->row_id == num_param_parts - 1) && (num_params % num_param_per_part != 0)) {
          row->params.resize(num_params % num_param_per_part);
        } else {
          row->params.resize(num_param_per_part);
        }
      });

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
            std::vector<float> step_sum(num_params, 0);
            int correct_count = 0;

            // 1. prepare params
            auto begin_time = std::chrono::steady_clock::now();
            std::vector<std::shared_ptr<TypedPartition<DenseRow>>> with_parts(num_param_parts);
            int start_idx =
                rand() %
                num_param_parts; // random start_idx to avoid overload on one point
            for (int i = 0; i < num_param_parts; i++) {
              int idx = (start_idx + i) % num_param_parts;
              auto part = typed_cache->GetPartition(idx);
              with_parts[idx] = std::dynamic_pointer_cast<TypedPartition<DenseRow>>(part);
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
                int k = iter1->row_id;
                auto& params = iter1->params;
                if (k == num_param_parts - 1 && num_params % num_param_per_part != -1) {
                  CHECK_EQ(params.size(),  num_params % num_param_per_part);
                } else {
                  CHECK_EQ(params.size(), num_param_per_part);
                }
                std::copy(params.begin(), params.end(), old_params.begin() + k*num_param_per_part);
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

            }  // replicate

            // create the output
            std::vector<std::pair<int, std::vector<float>>> kvs(num_param_parts);
            for (int i = 0 ; i < num_param_parts - 1; ++ i) {
              kvs[i].first = i;
              kvs[i].second.resize(num_param_per_part);
              auto begin = step_sum.begin() + i*num_param_per_part;
              auto end = step_sum.begin() + (i+1)*num_param_per_part;
              std::transform(begin, end, kvs[i].second.begin(), [count](float v) { return v/count; });
            }
            auto last_part_id = num_param_parts - 1;
            kvs[last_part_id].first = last_part_id;
            if (num_params % num_param_per_part != 0) {
              kvs[last_part_id].second.resize(num_params % num_param_per_part);
            } else {
              kvs[last_part_id].second.resize(num_param_per_part);
            }
            auto begin = step_sum.begin() + last_part_id*num_param_per_part;
            std::transform(begin, step_sum.end(), kvs[last_part_id].second.begin(), [count](float v) { return v/count; });
            
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
