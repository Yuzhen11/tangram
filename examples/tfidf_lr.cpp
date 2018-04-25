#include "base/color.hpp"
#include "boost/tokenizer.hpp"
#include "core/plan/runner.hpp"

#include <boost/algorithm/string.hpp>
#include <cmath>
#include <regex>
#include <string>

#include "examples/lr/basic_lr.hpp"

// #define ENABLE_CP

DEFINE_int32(num_of_docs, 1, "# number of docs");
DEFINE_int32(num_doc_partition, 10, "");
DEFINE_int32(num_term_partition, 10, "");

// lr
DEFINE_bool(is_sgd, false, "Full gradient descent or mini-batch SGD");

using namespace xyz;

class Document {
public:
  using KeyT = std::string;
  Document() = default;
  explicit Document(const KeyT &t) : title(t) {}
  KeyT title;
  std::vector<float> tf;
  std::vector<float> tf_idf;
  std::vector<size_t> words;
  int label; // for training
  int total_words = 0;
  const KeyT &id() const { return title; }
  KeyT Key() const { return title; }
  friend SArrayBinStream &operator<<(SArrayBinStream &stream,
                                     const Document &doc) {
    stream << doc.title << doc.tf << doc.tf_idf << doc.words << doc.total_words
           << doc.label;
    return stream;
  }
  friend SArrayBinStream &operator>>(SArrayBinStream &stream, Document &doc) {
    stream >> doc.title >> doc.tf >> doc.tf_idf >> doc.words >>
        doc.total_words >> doc.label;
    return stream;
  }
};

class Term {
public:
  using KeyT = std::size_t;
  Term() = default;
  explicit Term(const KeyT &term) : termid(term) {}
  KeyT termid;
  int idf;
  const KeyT &id() const { return termid; }
  KeyT Key() const { return termid; }
  friend SArrayBinStream &operator<<(SArrayBinStream &stream, const Term &t) {
    stream << t.termid << t.idf;
    return stream;
  }
  friend SArrayBinStream &operator>>(SArrayBinStream &stream, Term &t) {
    stream >> t.termid >> t.idf;
    return stream;
  }
};

// lr
static std::pair<int, int> sgd_train(TypedPartition<Document> *p, bool is_sgd,
                                     std::vector<float> &old_params,
                                     std::vector<float> &step_sum, double alpha,
                                     int num_params) {
  auto data_iter = p->begin();
  int count = 0, correct_count = 0;
  int sgd_counter = -1;
  auto end_iter = p->end();
  while (data_iter != end_iter) {
    // sgd: pick 1 point out of 10
    if (is_sgd) {
      sgd_counter++;
      if (sgd_counter % 10 != 0)
        continue;
    }

    auto &words = data_iter->words;
    auto &tf_idf = data_iter->tf_idf;
    CHECK_EQ(words.size(), tf_idf.size());
    int y = data_iter->label;

    float pred_y = 0.0;
    for (int i = 0; i < tf_idf.size(); i++) {
      pred_y += old_params[words[i]] * tf_idf[i];
    }
    pred_y += old_params[num_params - 1]; // intercept
    pred_y = 1. / (1. + exp(-1 * pred_y));

    // Checked and found that half of y is 0 & half is 1, but why the
    // correct_count is always 0 ???
    if ((y == 0 && pred_y < 0.5) || (y == 1 && pred_y >= 0.5)) {
      correct_count++;
    }

    for (int i = 0; i < tf_idf.size(); i++) {
      step_sum[words[i]] += alpha * tf_idf[i] * (y - pred_y);
    }
    step_sum[num_params - 1] += alpha * (y - pred_y); // intercept
    count++;

    ++data_iter;
  }
  return std::make_pair(count, correct_count);
}

int main(int argc, char **argv) {
  Runner::Init(argc, argv);
  const int combine_timeout = ParseCombineTimeout(FLAGS_combine_type);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "combine_type: " << FLAGS_combine_type
              << ", timeout: " << combine_timeout;
  }

  // load and generate two collections
  auto loaded_docs =
      Context::load_wholefiles(FLAGS_url, [](std::string content) {
        // parse extract title
        std::regex rgx(".*?title=\"(.*?)\".*?");
        std::smatch match;
        std::string title;
        if (std::regex_search(content, match, rgx)) {
          // LOG(INFO) << match[1];
          title = match[1];
        } else {
          CHECK(false) << "cannot match title";
        }

        Document doc(title);
        std::vector<int> count;
        if (content.size() > 0) {
          boost::char_separator<char> sep(" \t\n.,()\'\":;!?<>");
          boost::tokenizer<boost::char_separator<char>> tok(content, sep);
          for (auto &w : tok) {
            doc.words.push_back(std::hash<std::string>{}(w) % FLAGS_num_params);
            // std::transform(doc.words.back().begin(), doc.words.back().end(),
            // doc.words.back().begin(), ::tolower);
          }
          doc.total_words = doc.words.size();
          std::sort(doc.words.begin(), doc.words.end());
          int n = 0;
          for (int i = 0, j = 0; i < doc.words.size(); i = j) {
            for (j = i + 1; j < doc.words.size(); j++) {
              if (doc.words.at(i) != doc.words.at(j))
                break;
            }
            count.push_back(j - i);
            doc.words[n++] = doc.words[i];
          }
          doc.words.resize(n);
          doc.words.shrink_to_fit();
          doc.tf.resize(doc.words.size());
          doc.tf_idf.resize(doc.words.size());
          for (int i = 0; i < doc.words.size(); i++) {
            doc.tf.at(i) = static_cast<float>(count.at(i)) / doc.total_words;
          }
          doc.label = rand() % 2; // random label
        }

        return doc;
      });

  // no need indexed_docs using pull model.
  // auto indexed_docs =
  // Context::placeholder<Document>(FLAGS_num_doc_partition);
  auto terms_key_part_mapper =
      std::make_shared<HashKeyToPartMapper<size_t>>(FLAGS_num_term_partition);
  auto terms = Context::placeholder<Term>(FLAGS_num_term_partition,
                                          terms_key_part_mapper);
  /*
  Context::mappartjoin(
      loaded_docs, indexed_docs,
      [](TypedPartition<Document>* p, AbstractMapProgressTracker* t) {
        std::vector<std::pair<std::string, Document>> ret;
        for (auto& doc : *p) {
          ret.push_back({doc.id(),doc});
        }
        return ret;
      },
      [](Document *p_doc, Document doc) {
        *p_doc = std::move(doc);
      })
      ->SetName("build indexed_docs from loaded_docs");
  */

  // Context::sort_each_partition(indexed_docs);
  Context::sort_each_partition(terms);

  Context::mappartjoin(
      loaded_docs, terms,
      [](TypedPartition<Document> *p, AbstractMapProgressTracker *t) {
        std::vector<std::pair<size_t, int>> ret;
        for (auto &doc : *p) {
          for (int i = 0; i < doc.words.size(); i++) {
            ret.push_back({doc.words[i], 1});
          }
        }
        return ret;
      },
      [](Term *term, int n) {
        term->idf += n;

      })
      ->SetCombine([](int *a, int b) { *a += b; })
      ->SetName("Out all the doc");

  // Context::count(terms);
  auto dummy_collection = Context::placeholder<CountObjT>(1);
  Context::mappartwithjoin(
      loaded_docs, terms, dummy_collection,
      [terms_key_part_mapper](TypedPartition<Document> *p,
                              TypedCache<Term> *typed_cache,
                              AbstractMapProgressTracker *t) {

        std::vector<std::pair<int, int>> ret;
        std::vector<std::shared_ptr<IndexedSeqPartition<Term>>> with_parts(
            FLAGS_num_term_partition);
        std::map<std::string, int> terms_map;
        int start_idx = rand() % FLAGS_num_term_partition; // random start_idx
                                                           // to avoid overload
                                                           // on one point
        for (int i = 0; i < FLAGS_num_term_partition; i++) {
          int idx = (start_idx + i) % FLAGS_num_term_partition;
          auto start_time = std::chrono::system_clock::now();
          auto part = typed_cache->GetPartition(idx);
          auto end_time = std::chrono::system_clock::now();
          std::chrono::duration<double> duration = end_time - start_time;
          // if (FLAGS_node_id == 0) {
          //   LOG(INFO) << GREEN("fetch time for " + std::to_string(idx) << " :
          //   " + std::to_string(duration.count()));
          // }
          with_parts[idx] =
              std::dynamic_pointer_cast<IndexedSeqPartition<Term>>(part);
        }
        // LOG(INFO) << "fetch done";
        // method1: copy and find

        /*
        for (auto with_p : with_parts) {
          for (auto& term : *with_p) {
            terms_map[term.id()] = term.idf;
          }
        }

        LOG(INFO) << "building local terms_map";
        for (auto& doc : *p) {
          for(int i = 0; i < doc.words.size(); i++){
            std::string term  = doc.words[i];
            int count = terms_map[term];
            doc.tf_idf[i] = doc.tf[i] * std::log(FLAGS_num_of_docs /
        float(count));
          }
        }
        */

        // method2
        for (auto &doc : *p) {
          for (int i = 0; i < doc.words.size(); i++) {
            size_t term = doc.words[i];
            auto &with_p = with_parts[terms_key_part_mapper->Get(term)];
            auto *t = with_p->Find(term);
            CHECK_NOTNULL(t);
            int count = t->idf;
            doc.tf_idf[i] =
                doc.tf[i] * std::log(FLAGS_num_of_docs / float(count));
          }
        }

        for (int i = 0; i < FLAGS_num_term_partition; i++) {
          typed_cache->ReleasePart(i);
        }
        return ret;
      },
      [](CountObjT *t, int) {})
      ->SetName("Send idf back to doc");

  // Logistic regression
  int num_params = FLAGS_num_params + 2;
  double alpha = FLAGS_alpha;
  const int num_param_per_part = FLAGS_num_param_per_part;
  bool is_sgd = FLAGS_is_sgd;

  int num_param_parts = num_params / num_param_per_part;
  if (num_params % num_param_per_part != 0) {
    num_param_parts += 1;
  }
  // there are num_param_parts elements and num_param_parts partition.
  // the partition number does not need to be num_param_parts.
  auto dense_rows =
      create_dense_rows(num_param_parts, num_params, num_param_per_part);

  // auto params = Context::placeholder<Param>(num_param_parts);
  auto p1 =
      Context::mappartwithjoin(
          loaded_docs, dense_rows, dense_rows,
          [num_params, alpha, is_sgd, num_param_parts, num_param_per_part](
              TypedPartition<Document> *p, TypedCache<DenseRow> *typed_cache,
              AbstractMapProgressTracker *t) {
            std::vector<float> step_sum(num_params, 0);

            // 1. prepare params
            auto begin_time = std::chrono::steady_clock::now();
            auto with_parts = prepare_lr_params<DenseRow>(num_param_parts, typed_cache);
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
            auto old_params = copy_lr_params(with_parts, num_param_parts,
                                             num_params, num_param_per_part);

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
            auto count_correct =
                sgd_train(p, is_sgd, old_params, step_sum, alpha, num_params);
            int count = count_correct.first;
            int correct_count = count_correct.second;

            // create the output
            auto kvs = create_lr_output(num_param_parts, num_param_per_part,
                                        num_params, count, step_sum);

            LOG_IF(INFO, p->id == 0) << RED(
                "Correct: " + std::to_string(correct_count) + ", Batch size: " +
                std::to_string(count) + ", Accuracy: " +
                std::to_string(correct_count / float(count)) + " on part 0");

            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - begin_time);
            LOG_IF(INFO, p->id == 0)
                << GREEN("Computation time: " +
                         std::to_string(duration.count()) + "ms on part 0");

            for (int i = 0; i < num_param_parts; i++) {
              typed_cache->ReleasePart(i);
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

  // lr2
  auto dense_rows2 =
      create_dense_rows(num_param_parts, num_params, num_param_per_part);
  auto p2 =
      Context::mappartwithjoin(
          loaded_docs, dense_rows2, dense_rows2,
          [num_params, alpha, is_sgd, num_param_parts, num_param_per_part](
              TypedPartition<Document> *p, TypedCache<DenseRow> *typed_cache,
              AbstractMapProgressTracker *t) {
            std::vector<float> step_sum(num_params, 0);

            // 1. prepare params
            auto begin_time = std::chrono::steady_clock::now();
            auto with_parts = prepare_lr_params<DenseRow>(num_param_parts, typed_cache);
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
            auto old_params = copy_lr_params(with_parts, num_param_parts,
                                             num_params, num_param_per_part);

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
            auto count_correct =
                sgd_train(p, is_sgd, old_params, step_sum, alpha, num_params);
            int count = count_correct.first;
            int correct_count = count_correct.second;

            // create the output
            auto kvs = create_lr_output(num_param_parts, num_param_per_part,
                                        num_params, count, step_sum);

            LOG_IF(INFO, p->id == 0) << RED(
                "Correct: " + std::to_string(correct_count) + ", Batch size: " +
                std::to_string(count) + ", Accuracy: " +
                std::to_string(correct_count / float(count)) + " on part 0");

            end_time = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - begin_time);
            LOG_IF(INFO, p->id == 0)
                << GREEN("Computation time: " +
                         std::to_string(duration.count()) + "ms on part 0");

            for (int i = 0; i < num_param_parts; i++) {
              typed_cache->ReleasePart(i);
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

  Runner::Run();
}
