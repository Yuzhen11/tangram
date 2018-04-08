#include "core/plan/runner.hpp"

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

  auto load = [](std::string s) {
    std::pair<int, double> p;
    boost::char_separator<char> sep(" \t");
    boost::tokenizer<boost::char_separator<char>> tok(s, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
    p.first = std::stoi(*it++);
    p.second = std::stof(*it);
    return p;
  };
  auto pr1 = Context::load(FLAGS_url1, load);
  auto pr2 = Context::load(FLAGS_url2, load);

  auto pr_pair = Context::placeholder<PRpair>(5);

  Context::mapjoin(pr1, pr_pair, 
    [](std::pair<int, double> p) {
      return p;
    },
    [](PRpair* p, double a) {
      p->pr1 = a;
    });

  Context::mapjoin(pr2, pr_pair, 
    [](std::pair<int, double> p) {
      return p;
    },
    [](PRpair* p, double a) {
      p->pr2 = a;
    });

  auto diff = Context::placeholder<Diff>(1);
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
