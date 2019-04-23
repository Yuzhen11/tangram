#include "examples/lr/basic_lr.hpp"

int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  // load and generate two collections
  auto dataset = load_data();
  auto points = repartition(dataset);

  int num_params = FLAGS_num_params + 2;
  const int batch_size = FLAGS_batch_size;
  float alpha = FLAGS_alpha / batch_size;
  const int local_batch_size = batch_size / FLAGS_num_data_parts;
  CHECK_GT(local_batch_size, 0);
  LOG_IF(INFO, FLAGS_node_id == 0) << "local_batch_size: " << local_batch_size;

  // auto params = Context::placeholder<Param>(num_param_parts);
  const int num_param_per_part = FLAGS_num_param_per_part;

  int num_param_parts = num_params / num_param_per_part;
  if (num_params % num_param_per_part != 0) {
    num_param_parts += 1;
  }
  auto params = create_range_params(num_params, num_param_per_part);

  auto p =
      Context::mappartwithjoin(
          points, params, params,
          [num_params, alpha, local_batch_size](
              TypedPartition<IndexedPoints> *p, TypedCache<Param> *typed_cache,
              Output<int, float> *o) {
            // 1. Prepare keys
            auto begin_time = std::chrono::steady_clock::now();

            auto data_iter = p->begin();
            auto end_iter = p->end();
            // make sure there is local data
            CHECK(data_iter != end_iter);
            CHECK_GT(data_iter->points.size(), 0);
            std::vector<int> keys;
            int num_data = 0;
            while (num_data < local_batch_size) {
              for (auto &point : data_iter->points) {
                for (auto &f : point.x) {
                  keys.push_back(f.first);
                }
                num_data += 1;
                if (num_data == local_batch_size) {
                  break;
                }
              }
              if (num_data == local_batch_size) {
                break;
              }
              ++data_iter;
              if (data_iter == end_iter) {
                data_iter = p->begin();
              }
            }
            std::sort(keys.begin(), keys.end());
            keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

            auto end_time = std::chrono::steady_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - begin_time);
            // LOG_IF(INFO, FLAGS_node_id == 0) << GREEN("Parameter prepare
            // time: " + std::to_string(duration.count()));
            LOG_IF(INFO, p->id == 0) << GREEN(
                "Parameter prepare time: " + std::to_string(duration.count()) +
                "ms on part 0, params size: " + std::to_string(keys.size()));

            // 2. Get
            begin_time = std::chrono::steady_clock::now();
            auto pulled_objs = typed_cache->Get(keys);
            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - begin_time);
            LOG_IF(INFO, p->id == 0)
                << GREEN("Parameter Get time: " +
                         std::to_string(duration.count()) + "ms on part 0");

            // std::stringstream ss;
            // for (auto obj : pulled_objs) {
            //   ss << obj.fea << ": " << obj.val << ", ";
            // }
            // LOG(INFO) << ss.str();

            // 3. Calc grad
            begin_time = std::chrono::steady_clock::now();
            int correct_count = 0;
            std::vector<std::pair<int, float>> delta(keys.size());
            for (int i = 0; i < delta.size(); ++i) {
              delta[i].first = keys[i];
              delta[i].second = 0.;
            }

            data_iter = p->begin();
            end_iter = p->end();
            num_data = 0;
            while (num_data < local_batch_size) {
              for (auto &point : data_iter->points) {
                // calc gradient
                float y = point.y;
                if (y < 0)
                  y = 0.;
                float pred_y = 0.;
                int i = 0;
                for (auto &f : point.x) {
                  while (pulled_objs[i].fea < f.first) {
                    i += 1;
                  }
                  pred_y += pulled_objs[i].val * f.second;
                }
                pred_y = 1. / (1. + exp(-1 * pred_y));
                if ((y == 0 && pred_y < 0.5) || (y == 1 && pred_y >= 0.5)) {
                  correct_count++;
                }
                i = 0;
                for (auto &f : point.x) {
                  while (pulled_objs[i].fea < f.first) {
                    i += 1;
                  }
                  delta[i].second += alpha * f.second * (y - pred_y);
                }

                num_data += 1;
                if (num_data == local_batch_size) {
                  break;
                }
              }
              if (num_data == local_batch_size) {
                break;
              }
              ++data_iter;
              if (data_iter == end_iter) {
                data_iter = p->begin();
              }
            }
            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - begin_time);
            LOG_IF(INFO, p->id == 0)
                << GREEN("calculate time: " + std::to_string(duration.count()) +
                         "ms on part 0");
            LOG_IF(INFO, p->id == 0) << RED(
                "Correct: " + std::to_string(correct_count) + ", Batch size: " +
                std::to_string(local_batch_size) + ", Accuracy: " +
                std::to_string(correct_count / float(local_batch_size)));
            for (auto p : delta) {
              o->Add(p.first, p.second);
            }
          },
          [](Param *param, float val) { param->val += val; })
          ->SetIter(FLAGS_num_iter)
          ->SetStaleness(FLAGS_staleness)
          ->SetCombine([](float *a, float b) { *a = *a + b; });

  // Context::count(params);
  // Context::count(dataset);
  Runner::Run();
}
