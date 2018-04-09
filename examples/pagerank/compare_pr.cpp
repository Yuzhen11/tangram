#include "core/plan/runner.hpp"

#include "core/partition/block_partition.hpp"

#include "boost/tokenizer.hpp"

DEFINE_string(url1, "", "");
DEFINE_string(url2, "", "");

using namespace xyz;

struct PRpair {
  int id;
  double pr1;
  double pr2;

  using KeyT = int;
  PRpair() = default;
  PRpair(int _id) : id(_id) {}
  KeyT Key() const { return id; }
};

struct Diff {
  int id;
  double diff;

  using KeyT = int;
  Diff() = default;
  Diff(int _id) : id(_id) {}
  KeyT Key() const { return id; }
};

int main(int argc, char** argv) {
  Runner::Init(argc, argv);
  CHECK(FLAGS_url1.size());
  CHECK(FLAGS_url2.size());


  auto lines1 = Context::load_block_meta(FLAGS_url1);
  auto lines2 = Context::load_block_meta(FLAGS_url2);
  auto pr_pair = Context::placeholder<PRpair>(100);
  auto diff = Context::placeholder<Diff>(1);
  // lines1 -> pr_pair
  Context::mappartjoin(lines1, pr_pair,
    [](TypedPartition<std::string>* p, AbstractMapProgressTracker* t) {
      auto* bp = dynamic_cast<BlockPartition*>(p);
      CHECK_NOTNULL(bp);
      std::vector<std::pair<int, double>> kvs;
      auto reader = bp->GetReader();
      while (reader->HasLine()) {
        auto line = reader->GetLine();
        // LOG(INFO) << "line: " << line;
        boost::char_separator<char> sep(" \t\n");
        boost::tokenizer<boost::char_separator<char>> tok(line, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = std::stoi(*it++);
        double d = std::stof(*it);
        kvs.push_back({id, d});
      }
      return kvs;
    },
    [](PRpair* p, double c) {
      p->pr1 = c;
    });

  // lines2 -> pr_pair
  Context::mappartjoin(lines2, pr_pair,
    [](TypedPartition<std::string>* p, AbstractMapProgressTracker* t) {
      auto* bp = dynamic_cast<BlockPartition*>(p);
      CHECK_NOTNULL(bp);
      std::vector<std::pair<int, double>> kvs;
      auto reader = bp->GetReader();
      while (reader->HasLine()) {
        auto line = reader->GetLine();
        // LOG(INFO) << "line: " << line;
        boost::char_separator<char> sep(" \t\n");
        boost::tokenizer<boost::char_separator<char>> tok(line, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = std::stoi(*it++);
        double d = std::stof(*it);
        kvs.push_back({id, d});
      }
      return kvs;
    },
    [](PRpair* p, double c) {
      p->pr2 = c;
    });

  Context::mapjoin(pr_pair, diff,
    [](PRpair p) {
      // LOG(INFO) << "pr1, pr2, diff: " 
      //   << p.pr1 << " " << p.pr2 << " " << fabs(p.pr2 - p.pr1);
      return std::make_pair(0, fabs(p.pr2 - p.pr1));
    },
    [](Diff* d, double a) {
      d->diff += a;
    })
  ->SetCombine([](double* a, double b) { *a += b; });

  Context::foreach(diff, [](Diff d) {
    LOG(INFO) << RED("The difference is: " + std::to_string(d.diff));
  });

  Runner::Run();
}
