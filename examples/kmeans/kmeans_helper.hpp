#include "base/color.hpp"
#include "core/plan/runner.hpp"
#include "boost/tokenizer.hpp"

#include <string>
#include <cmath>

DEFINE_string(url, "", "The url for hdfs file");
DEFINE_int32(num_data, -1, "The number of data in the dataset");
DEFINE_int32(num_dims, -1, "The dimension of the dataset");
DEFINE_int32(K, 1, "The K value of kmeans, number of clusters");
DEFINE_int32(num_data_parts, -1, "The number of partitions for dataset");
DEFINE_int32(num_param_per_part, -1, "The number of parameters per partition");
DEFINE_int32(batch_size, 1, "Batch size of SGD");
DEFINE_double(alpha, 0.1, "The learning rate of the model");
DEFINE_int32(num_iter, 1, "The number of iterations");
DEFINE_int32(staleness, 0, "Staleness for the SSP");
DEFINE_bool(is_sgd, false, "Full gradient descent or mini-batch SGD");
DEFINE_string(combine_type, "kDirectCombine", "kShuffleCombine, kDirectCombine, kNoCombine, timeout");
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

static auto* load_data() {
  return Context::load(FLAGS_url, [](std::string s) {
	  Point point;
	  char* pos;
	  char* tok = strtok_r(&s[0], " \t:", &pos);
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
  }, FLAGS_max_lines_per_part)->SetName("dataset");
}

// return ID of cluster whose center is the nearest (uses euclidean distance), and the distance
std::pair<int, float> get_nearest_center(const std::vector<std::pair<int, float>>& x, int K,
                                         const std::vector<float>& params, int num_dims) {
  float square_dist, min_square_dist = std::numeric_limits<float>::max();
  int id_cluster_center = -1;

  for (int i = 0; i < K; i++)  // calculate the dist between point and clusters[i]
  {
    std::vector<float>::const_iterator first = params.begin() + i * num_dims;
    std::vector<float>::const_iterator last = params.begin() + (i + 1) * num_dims;
    std::vector<float> diff(first, last);

    for (auto field : x)
      diff[field.first] -= field.second;  // first:fea, second:val

    square_dist = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
    if (square_dist < min_square_dist) {
      min_square_dist = square_dist;
      id_cluster_center = i;
    }
  }
  return std::make_pair(id_cluster_center, min_square_dist);
}
