#include "base/color.hpp"
#include "boost/tokenizer.hpp"
#include "core/plan/runner.hpp"

#include "core/index/range_key_to_part_mapper.hpp"

#include <cmath>
#include <string>

DEFINE_string(url, "", "The url for hdfs file");
DEFINE_int32(num_data, -1, "The number of data in the dataset");
DEFINE_int32(num_params, -1, "The number of parameters in the dataset");
DEFINE_int32(num_data_parts, -1, "The number of partitions for dataset");
DEFINE_int32(num_param_per_part, -1, "The number of parameters per partition");
DEFINE_int32(batch_size, 1, "Batch size of SGD");
DEFINE_double(alpha, 0.1, "The learning rate of the model");
DEFINE_int32(num_iter, 1, "The number of iterations");
DEFINE_int32(staleness, 0, "Staleness for the SSP");
DEFINE_string(combine_type, "kDirectCombine",
              "kShuffleCombine, kDirectCombine, kNoCombine, timeout");
DEFINE_int32(max_lines_per_part, -1, "max lines per part, for debug");
DEFINE_int32(replicate_factor, 1, "replicate the dataset");

using namespace xyz;

struct Point {
  Point() = default;
  // <Fea, Val>
  std::vector<std::pair<int, float>> x;
  // Label
  int y;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Point &point) {
    stream << point.y << point.x;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Point &point) {
    stream >> point.y >> point.x;
    return stream;
  }
};

struct IndexedPoints {
  using KeyT = int;
  IndexedPoints() = default;
  IndexedPoints(KeyT _key) : key(_key) {}
  KeyT Key() const { return key; }
  KeyT key;
  std::vector<Point> points;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const IndexedPoints &indexed_points) {
    stream << indexed_points.key << indexed_points.points;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     IndexedPoints &indexed_points) {
    stream >> indexed_points.key >> indexed_points.points;
    return stream;
  }
};

struct Param {
  using KeyT = int;
  Param() = default;
  Param(KeyT _fea) : fea(_fea) {}
  KeyT Key() const { return fea; }
  KeyT fea;
  float val = 0;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Param &param) {
    stream << param.fea << param.val;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Param &param) {
    stream >> param.fea >> param.val;
    return stream;
  }
};

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

static auto *load_data() {
  return Context::load(FLAGS_url,
                       [](std::string s) {
                         Point point;
                         /*
                         boost::char_separator<char> sep(" \t");
                         boost::tokenizer<boost::char_separator<char>> tok(s,
                         sep);
                         boost::tokenizer<boost::char_separator<char>>::iterator
                         it = tok.begin();

                         point.y = std::stoi(*it);
                         it ++;
                         for (; it != tok.end(); ++it) {
                           std::vector<std::string> fea_val;
                           std::istringstream ss2(*it);
                           std::string token;
                           while (std::getline(ss2, token, ':')) {
                             fea_val.push_back(token);
                           }
                           CHECK_EQ(fea_val.size(), 2);
                           int fea = std::stoi(fea_val[0]);
                           float val = std::stof(fea_val[1]);
                           point.x.push_back(std::make_pair(fea, val));
                         }
                         */
                         char *pos;
                         char *tok = strtok_r(&s[0], " \t:", &pos);
                         int i = -1;
                         int idx;
                         float val;
                         while (tok != NULL) {
                           if (i == 0) {
                             idx = std::atoi(tok) - 1;
                             i = 1;
                           } else if (i == 1) {
                             val = std::atof(tok);
                             point.x.push_back(std::make_pair(idx, val));
                             i = 0;
                           } else {
                             point.y = std::atof(tok);
                             i = 0;
                           }
                           // Next key/value pair
                           tok = strtok_r(NULL, " \t:", &pos);
                         }

                         return point;
                       },
                       FLAGS_max_lines_per_part)
      ->SetName("dataset");
}

// Repartition the data
template <typename Collection> static auto *repartition(Collection *dataset) {
  int num_data_parts = FLAGS_num_data_parts;
  auto points =
      Context::placeholder<IndexedPoints>(num_data_parts)->SetName("points");
  Context::mappartjoin(
      dataset, points,
      [num_data_parts](TypedPartition<Point> *p, Output<int, Point> *o) {
        int i = rand() % num_data_parts;
        for (auto &v : *p) {
          o->Add(i, v);
          i += 1;
          if (i == num_data_parts) {
            i = 0;
          }
        }
      },
      [](IndexedPoints *ip, Point p) { ip->points.push_back(p); })
      ->SetName("construct points from dataset");

  return points;
}

static auto *create_range_params(const int num_params,
                                 const int num_param_per_part) {
  std::vector<third_party::Range> ranges;
  int num_param_parts = num_params / num_param_per_part;
  for (int i = 0; i < num_param_parts; ++i) {
    ranges.push_back(third_party::Range(i * num_param_per_part,
                                        (i + 1) * num_param_per_part));
  }
  if (num_params % num_param_per_part != 0) {
    ranges.push_back(
        third_party::Range(num_param_parts * num_param_per_part, num_params));
    num_param_parts += 1;
  }
  CHECK_EQ(ranges.size(), num_param_parts);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "num_param_parts: " << num_param_parts;
    for (auto range : ranges) {
      LOG(INFO) << "range: " << range.begin() << ", " << range.end();
    }
  }
  auto range_key_to_part_mapper =
      std::make_shared<RangeKeyToPartMapper<int>>(ranges);
  auto params = Context::range_placeholder<Param>(range_key_to_part_mapper);
  return params;
}

static auto *create_dense_rows(int num_param_parts, int num_params,
                               int num_param_per_part) {
  auto dense_rows = Context::placeholder<DenseRow>(num_param_parts);
  // init the param
  auto dummy_collection = Context::distribute<int>({1});
  Context::mapjoin(
      dummy_collection, dense_rows,
      [num_param_parts](int, Output<int, int> *o) {
        for (int i = 0; i < num_param_parts; ++i) {
          o->Add(i, 0);
        }
      },
      [num_param_parts, num_params, num_param_per_part](DenseRow *row, int) {
        if (row->row_id == num_param_parts - 1 &&
            num_params % num_param_per_part != 0) {
          row->params.resize(num_params % num_param_per_part);
        } else {
          row->params.resize(num_param_per_part);
        }
      })
      ->SetName("Create dense_rows for LR params");
  return dense_rows;
}

template <typename ObjT>
static auto prepare_lr_params(int num_param_parts,
                              TypedCache<ObjT> *typed_cache) {
  std::vector<std::shared_ptr<TypedPartition<ObjT>>> with_parts(
      num_param_parts);
  int start_idx =
      rand() %
      num_param_parts; // random start_idx to avoid overload on one point
  for (int i = 0; i < num_param_parts; i++) {
    int idx = (start_idx + i) % num_param_parts;
    auto part = typed_cache->GetPartition(idx);
    with_parts[idx] = std::dynamic_pointer_cast<TypedPartition<ObjT>>(part);
  }
  return with_parts;
}

// for DenseRow
static auto copy_lr_params(
    std::vector<std::shared_ptr<TypedPartition<DenseRow>>> &with_parts,
    int num_param_parts, int num_params, int num_param_per_part) {
  std::vector<float> old_params(num_params);
  for (auto with_p : with_parts) {
    auto iter1 = with_p->begin();
    auto end_iter = with_p->end();
    while (iter1 != end_iter) {
      int k = iter1->row_id;
      auto &params = iter1->params;
      if (k == num_param_parts - 1 && num_params % num_param_per_part != 0) {
        CHECK_EQ(params.size(), num_params % num_param_per_part);
      } else {
        CHECK_EQ(params.size(), num_param_per_part);
      }
      std::copy(params.begin(), params.end(),
                old_params.begin() + k * num_param_per_part);
      ++iter1;
    }
  }
  return old_params;
}

static auto create_lr_output(int num_param_parts, int num_param_per_part,
                             int num_params, int count,
                             std::vector<float> &step_sum) {
  std::vector<std::pair<int, std::vector<float>>> kvs(num_param_parts);
  for (int i = 0; i < num_param_parts - 1; ++i) {
    kvs[i].first = i;
    kvs[i].second.resize(num_param_per_part);
    auto begin = step_sum.begin() + i * num_param_per_part;
    auto end = step_sum.begin() + (i + 1) * num_param_per_part;
    std::transform(begin, end, kvs[i].second.begin(),
                   [count](float v) { return v / count; });
  }
  auto last_part_id = num_param_parts - 1;
  kvs[last_part_id].first = last_part_id;
  if (num_params % num_param_per_part != 0) {
    kvs[last_part_id].second.resize(num_params % num_param_per_part);
  } else {
    kvs[last_part_id].second.resize(num_param_per_part);
  }
  auto begin = step_sum.begin() + last_part_id * num_param_per_part;
  std::transform(begin, step_sum.end(), kvs[last_part_id].second.begin(),
                 [count](float v) { return v / count; });
  return kvs;
}
