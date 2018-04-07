#include "examples/lr/basic_lr.hpp"

int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  // load and generate two collections
  auto dataset = load_data();

  int num_params = FLAGS_num_params;
  double alpha = FLAGS_alpha;
  int num_parts = FLAGS_num_parts;
  int batch_size = FLAGS_batch_size;

  auto params = Context::placeholder<Param>(num_parts);
  auto p =
      Context::mappartwithjoin(
          dataset, params, params,
          [num_params, alpha, batch_size, num_parts](TypedPartition<Point> *p,
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
            CHECK_GT(num_data, batch_size);
            for (int i = 0; i < offset; i++)
            ++iter2;
            for (int i = 0; i < batch_size; i++) {
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