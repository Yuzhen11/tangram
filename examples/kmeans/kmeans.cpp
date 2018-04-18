#include "examples/kmeans/kmeans_helper.hpp"
#include "core/index/range_key_to_part_mapper.hpp"

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
  int num_data_parts = FLAGS_num_data_parts;
  auto points = Context::placeholder<IndexedPoints>(num_data_parts)->SetName("points");
  auto p0 = Context::mappartjoin(dataset, points,
    [num_data_parts](TypedPartition<Point>* p,
      AbstractMapProgressTracker* t) {
      std::vector<std::pair<int,Point>> all;
      for (auto& v: *p) {
        all.push_back(std::make_pair(rand() % num_data_parts, v));
      }
      return all;
    },
    [](IndexedPoints* ip, Point p) {
      ip->points.push_back(p);
    })
    ->SetName("construct points from dataset");

  // num_params = dimension * K + K (or dimension*(K+1) if we use a vector<vector<float>> params to store them)
  int K = FLAGS_K;
  int num_data = FLAGS_num_data;
  int num_dims = FLAGS_num_dims;
  int num_params = num_dims * (K + 1);
  double alpha = FLAGS_alpha;
  const int num_param_per_part = FLAGS_num_param_per_part;
  bool is_sgd = FLAGS_is_sgd;

  std::vector<third_party::Range> ranges;
  int num_param_parts = num_params / num_param_per_part;
  for (int i = 0; i < num_param_parts; ++ i) {
    ranges.push_back(third_party::Range(i * num_param_per_part, (i + 1) * num_param_per_part));
  }
  if (num_params % num_param_per_part != 0) {
    ranges.push_back(third_party::Range(num_param_parts * num_param_per_part, num_params));
    num_param_parts += 1;
  }
  CHECK_EQ(ranges.size(), num_param_parts);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "num_param_parts: " << num_param_parts;
    for (auto range: ranges) {
      LOG(INFO) << "range: " << range.begin() << ", " << range.end();
    }
  }
  auto range_key_to_part_mapper = std::make_shared<RangeKeyToPartMapper<int>>(ranges);
  auto params = Context::range_placeholder<Param>(range_key_to_part_mapper);

#ifdef ENABLE_CP
  Context::checkpoint(params, "/tmp/tmp/yz");
  Context::checkpoint(points, "/tmp/tmp/yz");
#endif


  auto init_points = Context::placeholder<IndexedPoints>(1)->SetName("init_points");
  auto p1 = Context::mappartjoin(dataset, init_points,
    [K, num_data_parts](TypedPartition<Point>* p,
      AbstractMapProgressTracker* t) {
      std::vector<std::pair<int, Point>> all;
      int num_local_data = 0;
      for (auto& v: *p)
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
        all.push_back(std::make_pair(0, *iter));
      }
      return all;
    },
    [](IndexedPoints* ip, Point p) {
      ip->points.push_back(p);
    })
    ->SetName("construct init_points from dataset");

  auto p2 = Context::mappartjoin(init_points, params,
    [K, num_dims](TypedPartition<IndexedPoints>* p,
      AbstractMapProgressTracker* t) {
      std::set<int> indexes; // K index for K clusters (points)
      std::vector<std::pair<int, float>> all; // <fea, val>

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

      auto points = p->begin()->points; // p only have 1 part here, points is of vector<Point>
      count = 0;
      for (auto index : indexes) {
        auto& x = points[index].x;
        for (auto field : x)
          all.push_back(std::make_pair(field.first + count * num_dims, field.second));

        all.push_back(std::make_pair(count + K * num_dims, 1));
        count++;
      }
      return all;
    },
    [](Param* param, float val) {
      param->val = val;
    })
    ->SetName("Init the K clusters");


  auto p3 =
      Context::mappartwithjoin(
          points, params, params,
          [num_params, num_dims, K, alpha, is_sgd,
           num_param_parts, range_key_to_part_mapper](TypedPartition<IndexedPoints> *p, TypedCache<Param> *typed_cache,
                      AbstractMapProgressTracker *t) {
            std::vector<std::pair<int, float>> kvs;
            std::vector<float> step_sum(num_params, 0);
            // int correct_count = 0;

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
            int count = 0;
            int sgd_counter = -1;
            int id_nearest_center;
            float learning_rate;
            // Test accuracy
            float mse = 0;  // mean sum of square error
            std::vector<int> cluster(K); // # of points in each cluster, use to tune alpha
            std::vector<float> deltas = old_params;

            // run FLAGS_replicate_factor time
            for (int replicate = 0; replicate < FLAGS_replicate_factor; ++ replicate) {
              auto iter2 = p->begin();
              auto end_iter = p->end();
              while (iter2 != end_iter) {
                for (auto& point : iter2->points) {
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

                  // learning_rate = alpha / ++deltas[FLAGS_K][id_nearest_center];
                  learning_rate = alpha / ++deltas[id_nearest_center + K * num_dims];

                  // The params of the id_nearest_center-th cluster point
                  std::vector<float>::const_iterator first = deltas.begin() + id_nearest_center * num_dims;
                  std::vector<float>::const_iterator last = deltas.begin() + (id_nearest_center + 1) * num_dims;
                  std::vector<float> distance(first, last);
                  for (auto field : x)
                    distance[field.first] -= field.second;  // first:fea, second:val

                  for (int j = 0; j < num_dims; j++)
                    deltas[id_nearest_center * num_dims + j] -= learning_rate * distance[j];

                  count++;
                }
                ++iter2;
              }
            }
            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time);
            LOG_IF(INFO, p->id == 0) << GREEN("Computation time: " + 
                          std::to_string(duration.count())
                          + "ms on part 0");

            for (int i = 0; i < num_param_parts; i++) {
              typed_cache->ReleasePart(i);
            }

            // update params
            for (int i = 0; i < num_params; ++i)
                deltas[i] -= old_params[i];

            for (int j = 0; j < num_params; j++) {
              kvs.push_back({j, deltas[j]});
            }
            
           LOG_IF(INFO, p->id == 0) << RED("Batch size: " + std::to_string(count) +
                             ", MSE: " + std::to_string(mse / count) + " on part 0");

           for (int i = 0; i < K; i++)  // for tuning learning rate
            LOG_IF(INFO, p->id == 0) << RED("Cluster " + std::to_string(i) + ": " + std::to_string(cluster[i]));
               
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
