#include "core/plan/runner.hpp"

#include "core/partition/block_partition.hpp"

#include "boost/tokenizer.hpp"

DEFINE_string(url, "", "");

using namespace xyz;

struct Sum {
  int id;
  double sum;

  using KeyT = int;
  Sum() = default;
  Sum(int _id) : id(_id) {}
  KeyT Key() const { return id; }
};

int main(int argc, char** argv) {
  Runner::Init(argc, argv);
  CHECK(FLAGS_url.size());

  auto lines = Context::load_block_meta(FLAGS_url);
  auto sum = Context::placeholder<Sum>(1);
  Context::mappartjoin(lines, sum,
    [](TypedPartition<std::string>* p, AbstractMapProgressTracker* t) {
      auto* bp = dynamic_cast<BlockPartition*>(p);
      CHECK_NOTNULL(bp);
      std::vector<std::pair<int, double>> kvs;
      auto reader = bp->GetReader();
      double sum = 0;
      while (reader->HasLine()) {
        auto line = reader->GetLine();
        // LOG(INFO) << "line: " << line;
        boost::char_separator<char> sep(" \t\n");
        boost::tokenizer<boost::char_separator<char>> tok(line, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        std::stoi(*it++);
        double d = std::stof(*it);
        sum += d;
      }
      kvs.push_back({0, sum});
      return kvs;
    },
    [](Sum* s, double c) {
      s->sum += c;
    })
  ->SetCombine(
      [](double * a, double b) { return *a += b; });

  Context::foreach(sum, [](Sum s) {
    LOG(INFO) << RED("The sum is: " + std::to_string(s.sum));
  });

  Runner::Run();
}

