#include "base/color.hpp"
#include "core/plan/runner.hpp"
#include "boost/tokenizer.hpp"

#include <string>
#include <cmath>

DEFINE_string(url, "", "The url for hdfs file");
DEFINE_int32(num_params, -1, "The number of parameters in the dataset");
DEFINE_int32(num_parts, -1, "The number of partitions");
DEFINE_int32(batch_size, 1, "Batch size of SGD");
DEFINE_double(alpha, 0.1, "The learning rate of the model");
DEFINE_int32(num_iter, 1, "The number of iterations");
DEFINE_int32(staleness, 0, "Staleness for the SSP");
DEFINE_bool(is_sparse, false, "Is the dataset sparse or not");
DEFINE_bool(is_sgd, false, "Full gradient descent or mini-batch SGD");

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
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();

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

    return point;
  })->SetName("dataset");
}