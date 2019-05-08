#include "core/plan/runner.hpp"

#include "core/partition/block_partition.hpp"

#include "boost/tokenizer.hpp"

DEFINE_string(url, "", "The url for hdfs file");
DEFINE_int32(num_parts, 100, "# word partitions");
DEFINE_string(combine_type, "kDirectCombine",
              "kShuffleCombine, kDirectCombine, kNoCombine, timeout");

using namespace xyz;

struct WC {
  using KeyT = std::string;
  KeyT word;
  int count = 0;

  WC() = default;
  WC(KeyT key) : word(key) {}
  KeyT Key() const { return word; }

  // TODO: we dont need the serialization func.
  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const WC &wc) {
    stream << wc.word << wc.count;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream, WC &wc) {
    stream >> wc.word >> wc.count;
    return stream;
  }
};

int main(int argc, char **argv) {
  Runner::Init(argc, argv);
  const int combine_timeout = ParseCombineTimeout(FLAGS_combine_type);
  if (FLAGS_node_id == 0) {
    LOG(INFO) << "combine_type: " << FLAGS_combine_type
              << ", timeout: " << combine_timeout;
  }

  // use load_block_meta, read the block in mappartupdate
  auto lines = Context::load_block_meta(FLAGS_url);
  auto wordcount = Context::placeholder<WC>(FLAGS_num_parts);
  Context::mappartupdate(
      lines, wordcount,
      [](TypedPartition<std::string> *p, Output<std::string, int> *o) {
        auto *bp = dynamic_cast<BlockPartition *>(p);
        CHECK_NOTNULL(bp);
        auto reader = bp->GetReader();
        while (reader->HasLine()) {
          auto line = reader->GetLine();
          // LOG(INFO) << "line: " << line;
          boost::char_separator<char> sep(" \t\n");
          boost::tokenizer<boost::char_separator<char>> tok(line, sep);
          for (auto &w : tok) {
            o->Add(w, 1);
          }
        }
        LOG(INFO) << p->id << " map done";
      },
      [](WC *wc, int c) { wc->count += c; })
      ->SetCombine([](int *a, int b) { return *a += b; }, combine_timeout);

  Context::count(wordcount);

  Runner::Run();
}
