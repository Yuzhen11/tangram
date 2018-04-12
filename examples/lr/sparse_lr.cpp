#include "examples/lr/basic_lr.hpp"

int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  // load and generate two collections
  auto dataset = load_data();

  int num_params = FLAGS_num_params + 2;
  double alpha = FLAGS_alpha;
  int num_param_parts = FLAGS_num_param_parts;
  int batch_size = FLAGS_batch_size;

  auto params = Context::placeholder<Param>(num_param_parts);
  auto p =
      Context::mappartwithjoin(
          dataset, params, params,
          [num_params, alpha, batch_size, num_param_parts](TypedPartition<Point> *p,
                                        TypedCache<Param> *typed_cache,
                                        AbstractMapProgressTracker *t) {
            std::vector<std::pair<int, float>> kvs;
            std::vector<float> step_sum(num_params, 0);
            int correct_count = 0;
            int num_data = 0;

            auto iter0 = p->begin();
            while (iter0 != p->end()) {
              num_data++;
              ++iter0;
            }

            // mini-batch starting position
            int offset = rand() % (num_data - batch_size);

            std::set<int> keys_set;
            std::map<int, float> old_params;
            auto iter3 = p->begin();
            CHECK_GT(num_data, batch_size);
            for (int i = 0; i < offset; i++)
              ++iter3;
            for (int i = 0; i < batch_size; i++) {
              auto &x = iter3->x;
              for (auto field : x) {
                keys_set.insert(field.first);
              }
              ++iter3;
            }
            keys_set.insert(num_params - 1);
            std::vector<int> keys(keys_set.begin(), keys_set.end());
            auto objs = typed_cache->Get(keys);
            CHECK_EQ(keys.size(), objs.size());
            for (int i = 0; i < keys.size(); i++)
              old_params.insert(std::make_pair(objs[i].fea, objs[i].val));

            auto iter4 = p->begin();
            for (int i = 0; i < offset; i++)
              ++iter4;
            for (int i = 0; i < batch_size; i++) {
              auto &x = iter4->x;
              auto y = iter4->y;
              if (y < 0)
                y = 0;

              float pred_y = 0.0;
              for (auto field : x) {
                CHECK(old_params.find(field.first) != old_params.end());
                pred_y += old_params[field.first] * field.second;
              }
              CHECK(old_params.find(num_params - 1) != old_params.end());
              pred_y += old_params[num_params - 1]; // intercept
              pred_y = 1. / (1. + exp(-1 * pred_y));

              if ((y == 0 && pred_y < 0.5) || (y == 1 && pred_y >= 0.5)) {
                correct_count++;
              }
              for (auto field : x) {
                step_sum[field.first] += alpha * field.second * (y - pred_y);
              }
              step_sum[num_params - 1] += alpha * (y - pred_y); // intercept
              ++iter4;
            }
            
            for (int i = 0; i < num_params; i++) {
              step_sum[i] /= num_params;
            }
            for (int j = 0; j < num_params; j++) {
              kvs.push_back({j, step_sum[j]});
            }
            LOG(INFO) << RED("Correct: " + std::to_string(correct_count) +
                             ", Batch size: " + std::to_string(batch_size) +
                             ", Accuracy: " +
                             std::to_string(correct_count / float(batch_size)));
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
