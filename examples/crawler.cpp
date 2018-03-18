#include "core/plan/runner.hpp"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "proj10", "The namenode of hdfs");
DEFINE_int32(hdfs_port, 9000, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

using namespace xyz;

struct UrlElem {
  using KeyT = std::string;
  UrlElem() = default;
  UrlElem(KeyT k) : url(k), status(Status::ToFetch) {}

  KeyT Key() const { return url; }
  enum class Status : char {
      ToFetch, Done
  };

  KeyT url;
  Status status;

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const UrlElem& url_elem) {
    stream << url_elem.url << url_elem.status;
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, UrlElem& url_elem) {
    stream >> url_elem.url >> url_elem.status;
    return stream;
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << "url: " << url;
    ss << ", status: " << (status == Status::ToFetch ? "ToFetch":"Done");
    return ss.str();
  }
};


int main(int argc, char** argv) {
  Runner::Init(argc, argv);
  std::vector<UrlElem> seeds{UrlElem("url1"), UrlElem("url2")};
  auto url_table = Context::distribute_by_key(seeds, 1, "distribute the seed");

  Context::mapjoin(url_table, url_table, 
    [](const UrlElem& url_elem) {
      std::vector<std::pair<std::string, UrlElem::Status>> ret;
      if (url_elem.status == UrlElem::Status::ToFetch) {
        // TODO: download the page and extract url
        ret.push_back({url_elem.url, UrlElem::Status::Done});
        ret.push_back({"url3", UrlElem::Status::ToFetch});
        ret.push_back({"url4", UrlElem::Status::ToFetch});
      }
      return ret;
    },
    [](UrlElem* url_elem, UrlElem::Status s) {
      if (s == UrlElem::Status::Done) {
        url_elem->status = UrlElem::Status::Done;
      }
    })->SetIter(2)->SetName("crawler main logic");
  Context::foreach(url_table, [](const UrlElem& url_elem) {
    LOG(INFO) << url_elem.DebugString();
  }, "print all status");
  Context::count(url_table);

  Runner::Run();
  // Runner::PrintDag();
}
