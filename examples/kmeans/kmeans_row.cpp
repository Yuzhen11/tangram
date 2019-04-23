#include "examples/kmeans/kmeans_helper.hpp"

using namespace xyz;

/*
 * Use row to store the centers, like Bosen.
 * Achieve better performance using row-based updates.
 * For small model size, we can just use 1 row to store all the parameters.
 *
 * To store all params in one row, set num_param_per_part >= num_dims*(K+1)
 */

// #define ENABLE_CP
//
struct DenseRow {
  using KeyT = int;
  DenseRow() = default;
  DenseRow(KeyT id) : row_id(id) {}
  KeyT Key() const { return row_id; }

  int row_id;
  std::vector<float> params;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const DenseRow &row) {
    stream << row.row_id << row.params;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     DenseRow &row) {
    stream >> row.row_id >> row.params;
    return stream;
  }
};

int main(int argc, char **argv) {
  Runner::Init(argc, argv);
  const int combine_timeout = ParseCombineTimeout(FLAGS_combine_type);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "combine_type: " << FLAGS_combine_type
              << ", timeout: " << combine_timeout;
  }

  // load and generate two collections
  auto dataset = load_data();

  // Repartition the data
  int num_data_parts = FLAGS_num_data_parts;
  auto points =
      Context::placeholder<IndexedPoints>(num_data_parts)->SetName("points");
  auto p0 = Context::mappartjoin(dataset, points,
                                 [num_data_parts](TypedPartition<Point> *p,
                                                  Output<int, Point> *o) {
                                   for (auto &v : *p) {
                                     o->Add(rand() % num_data_parts, v);
                                   }
                                 },
                                 [](IndexedPoints *ip, Point p) {
                                   // for (int i = 0; i < 10; ++ i) {
                                   ip->points.push_back(p);
                                   // }
                                 })
                ->SetName("construct points from dataset");

  // num_params = dimension * K + K (or dimension*(K+1) if we use a
  // vector<vector<float>> params to store them)
  int K = FLAGS_K;
  int num_data = FLAGS_num_data;
  int num_dims = FLAGS_num_dims;
  int num_params = num_dims * (K + 1); // TODO: require num_dims >= k
  double alpha = FLAGS_alpha;
  const int num_param_per_part = FLAGS_num_param_per_part;
  bool is_sgd = FLAGS_is_sgd;

  std::vector<third_party::Range> ranges;
  int num_param_parts = num_params / num_param_per_part;
  if (num_params % num_param_per_part != 0) {
    num_param_parts += 1;
  }
  // there are num_param_parts elements and num_param_parts partition.
  // the partition number does not need to be num_param_parts.
  auto dense_rows = Context::placeholder<DenseRow>(num_param_parts);

#ifdef ENABLE_CP
  // Context::checkpoint(params, "/tmp/tmp/yz");
  Context::checkpoint(points, "/tmp/tmp/yz");
#endif

  auto init_points =
      Context::placeholder<IndexedPoints>(1)->SetName("init_points");
  auto p1 =
      Context::mappartjoin(
          dataset, init_points,
          [K, num_data_parts](TypedPartition<Point> *p, Output<int, Point> *o) {
            int num_local_data = 0;
            for (auto &v : *p)
              num_local_data++;

            std::set<int> indexes; // K index for K clusters (points)
            int count = 0;
            while (count < K) {
              int tmp = rand() % num_local_data;
              if (indexes.find(tmp) == indexes.end()) {
                indexes.insert(tmp);
                count++;
              }
            }

            count = 0;
            auto iter = p->begin();
            for (auto index : indexes) {
              while (count < index) {
                ++count;
                ++iter;
              }
              o->Add(0, *iter);
            }
          },
          [](IndexedPoints *ip, Point p) { ip->points.push_back(p); })
          ->SetName("construct init_points from dataset");

  auto p2 =
      Context::mappartjoin(
          init_points, dense_rows,
          [K, num_dims, num_params, num_param_per_part,
           num_param_parts](TypedPartition<IndexedPoints> *p,
                            Output<int, std::vector<float>> *o) {
            std::set<int> indexes; // K index for K clusters (points)
            std::vector<std::pair<int, std::vector<float>>> ret(
                num_param_parts);

            int num_local_data = p->begin()->points.size();
            CHECK_LE(K, num_local_data);
            int count = 0;
            while (count < K) {
              int tmp = rand() % num_local_data;
              if (indexes.find(tmp) == indexes.end()) {
                indexes.insert(tmp);
                count++;
              }
            }

            std::vector<std::pair<int, float>> all; // <fea, val>
            auto points = p->begin()->points; // p only have 1 part here, points
                                              // is of vector<Point>
            count = 0;
            for (auto index : indexes) {
              auto &x = points[index].x;
              for (auto field : x)
                all.push_back(std::make_pair(field.first + count * num_dims,
                                             field.second));

              all.push_back(std::make_pair(count + K * num_dims, 1));
              count++;
            }

            for (int i = 0; i < ret.size(); ++i) {
              ret[i].first = i;
              ret[i].second.resize(num_param_per_part);
            }
            if (num_params % num_param_per_part != 0) {
              ret[ret.size() - 1].second.resize(num_params %
                                                num_param_per_part);
            }
            for (auto &p : all) {
              ret[p.first / num_param_per_part]
                  .second[p.first % num_param_per_part] = p.second;
            }
            for (int i = 0; i < ret.size(); ++i) {
              o->Add(ret[i].first, std::move(ret[i].second));
            }
          },
          [](DenseRow *row, std::vector<float> params) {
            row->params = params;
          })
          ->SetName("Init the K clusters");

  auto p3 =
      Context::mappartwithjoin(
          points, dense_rows, dense_rows,
          [num_params, num_dims, K, alpha, is_sgd, num_param_parts,
           num_param_per_part](TypedPartition<IndexedPoints> *p,
                               TypedCache<DenseRow> *typed_cache,
                               Output<int, std::vector<float>> *o) {
            // int correct_count = 0;

            // 1. prepare params
            auto begin_time = std::chrono::steady_clock::now();
            std::vector<std::shared_ptr<TypedPartition<DenseRow>>> with_parts(
                num_param_parts);
            int start_idx = rand() % num_param_parts; // random start_idx to
                                                      // avoid overload on one
                                                      // point
            for (int i = 0; i < num_param_parts; i++) {
              int idx = (start_idx + i) % num_param_parts;
              auto part = typed_cache->GetPartition(idx);
              with_parts[idx] =
                  std::dynamic_pointer_cast<TypedPartition<DenseRow>>(part);
            }

            // TOOD:FT for fetching map is not supported yet!
            // so make sure to kill after fetching
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("sleeping");
            // std::this_thread::sleep_for(std::chrono::seconds(1));

            auto end_time = std::chrono::steady_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - begin_time);
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("Parameter prepare
            // time: " + std::to_string(duration.count()));
            LOG_IF(INFO, p->id == 0)
                << GREEN("Parameter prepare time: " +
                         std::to_string(duration.count()) + "ms on part 0");

            // 2. copy params
            begin_time = std::chrono::steady_clock::now();
            std::vector<float> old_params(num_params);
            for (auto with_p : with_parts) {
              auto iter1 = with_p->begin();
              auto end_iter = with_p->end();
              while (iter1 != end_iter) {
                int row_id = iter1->row_id;
                auto &params = iter1->params;
                if (row_id == num_param_parts - 1 &&
                    num_params % num_param_per_part != 0) {
                  CHECK_EQ(params.size(), num_params % num_param_per_part);
                } else {
                  CHECK_EQ(params.size(), num_param_per_part);
                }
                std::copy(params.begin(), params.end(),
                          old_params.begin() + row_id * num_param_per_part);
                ++iter1;
              }
            }

            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - begin_time);
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("Parameter copy time: "
            // + std::to_string(duration.count()));
            LOG_IF(INFO, p->id == 0)
                << GREEN("Parameter copy time: " +
                         std::to_string(duration.count()) + "ms on part 0");

            // 3. calculate
            begin_time = std::chrono::steady_clock::now();
            int count = 0;
            int sgd_counter = -1;
            int id_nearest_center;
            float learning_rate;
            // Test accuracy
            float mse = 0; // mean sum of square error
            std::vector<int> cluster(
                K); // # of points in each cluster, use to tune alpha
            std::vector<float> deltas = old_params;

            // run FLAGS_replicate_factor time
            for (int replicate = 0; replicate < FLAGS_replicate_factor;
                 ++replicate) {
              auto iter2 = p->begin();
              auto end_iter = p->end();
              while (iter2 != end_iter) {
                for (auto &point : iter2->points) {
                  // sgd: pick 1 point out of 10
                  if (is_sgd) {
                    sgd_counter++;
                    if (sgd_counter % 40 != 0)
                      continue;
                  }

                  // kmeans update logic
                  auto &x = point.x;
                  auto id_dist = get_nearest_center(x, K, deltas, num_dims);
                  id_nearest_center = id_dist.first;

                  cluster[id_nearest_center]++;
                  mse += id_dist.second;

                  // learning_rate = alpha /
                  // ++deltas[FLAGS_K][id_nearest_center];
                  learning_rate =
                      alpha / ++deltas[id_nearest_center + K * num_dims];

                  // update delta
                  int begin = id_nearest_center * num_dims;
                  int j = 0;
                  for (auto &field : x) {
                    while (j < field.first) {
                      deltas[begin + j] -= learning_rate * (deltas[begin + j]);
                      j += 1;
                    }
                    deltas[begin + j] -=
                        learning_rate * (deltas[begin + j] - field.second);
                    j += 1;
                  }
                  while (j < num_dims) {
                    deltas[begin + j] -= learning_rate * (deltas[begin + j]);
                    j += 1;
                  }

                  count++;
                }
                ++iter2;
              }
            }
            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - begin_time);
            LOG_IF(INFO, p->id == 0)
                << GREEN("Computation time: " +
                         std::to_string(duration.count()) + "ms on part 0");

            for (int i = 0; i < num_param_parts; i++) {
              typed_cache->ReleasePart(i);
            }

            for (int i = 0; i < num_params; ++i)
              deltas[i] -= old_params[i];

            std::vector<std::pair<int, std::vector<float>>> kvs(
                num_param_parts);
            for (int i = 0; i < num_param_parts - 1; ++i) {
              kvs[i].first = i;
              kvs[i].second.resize(num_param_per_part);
              auto begin = deltas.begin() + i * num_param_per_part;
              auto end = deltas.begin() + (i + 1) * num_param_per_part;
              std::copy(begin, end, kvs[i].second.begin());
            }
            auto last_part_id = num_param_parts - 1;
            kvs[last_part_id].first = last_part_id;
            if (num_params % num_param_per_part != 0) {
              kvs[last_part_id].second.resize(num_params % num_param_per_part);
            } else {
              kvs[last_part_id].second.resize(num_param_per_part);
            }
            auto begin = deltas.begin() + last_part_id * num_param_per_part;
            std::copy(begin, deltas.end(), kvs[last_part_id].second.begin());

            LOG_IF(INFO, p->id == 0)
                << RED("Batch size: " + std::to_string(count) + ", MSE: " +
                       std::to_string(mse / count) + " on part 0");

            for (int i = 0; i < K; i++) // for tuning learning rate
              LOG_IF(INFO, p->id == 0)
                  << RED("Cluster " + std::to_string(i) + ": " +
                         std::to_string(cluster[i]));

            for (auto &kv : kvs) {
              o->Add(kv.first, std::move(kv.second));
            }
            return kvs;
          },
          [](DenseRow *row, std::vector<float> v) {
            CHECK_EQ(row->params.size(), v.size());
            for (int i = 0; i < v.size(); ++i) {
              row->params[i] += v[i];
            }
          })
          ->SetIter(FLAGS_num_iter)
          ->SetStaleness(FLAGS_staleness)
#ifdef ENABLE_CP
          ->SetCheckpointInterval(5, "/tmp/tmp/yz")
#endif
          ->SetCombine(
              [](std::vector<float> *v, const std::vector<float> &o) {
                CHECK_EQ(v->size(), o.size());
                for (int i = 0; i < v->size(); ++i) {
                  (*v)[i] += o[i];
                }
              },
              combine_timeout);

  // Context::count(params);
  // Context::count(points);
  Runner::Run();
}
