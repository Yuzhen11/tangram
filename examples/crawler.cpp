#include "core/plan/runner.hpp"
#include "base/color.hpp"

DEFINE_string(url, "", "The url to fetch");

DEFINE_string(python_script_path, "", "");

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
  std::vector<UrlElem> seeds{UrlElem(FLAGS_url)};
  auto url_table = Context::distribute_by_key(seeds, 20, "distribute the seed");

  Context::mapjoin(url_table, url_table, 
    [](const UrlElem& url_elem) {
      std::vector<std::pair<std::string, UrlElem::Status>> ret;
      if (url_elem.status == UrlElem::Status::ToFetch) {
        LOG(INFO) << RED("mapping");
    	  std::string cmd = "python " + FLAGS_python_script_path + " "+url_elem.url;
		    std::array<char, 128> buffer;
    	  std::string result;
    	  std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
    	  if (!pipe) throw std::runtime_error("popen() failed!");
    	  while (!feof(pipe.get())) {
          if (fgets(buffer.data(), 128, pipe.get()) != nullptr)
            result += buffer.data();
		  }
      std::stringstream ss(result);
      std::istream_iterator<std::string> begin(ss);
      std::istream_iterator<std::string> end;
      std::vector<std::string> urls(begin, end);
          // TODO: download the page and extract url
          ret.push_back({url_elem.url, UrlElem::Status::Done});
      for(auto fetched_url : urls){
            LOG(INFO) << RED(fetched_url);
            ret.push_back({fetched_url, UrlElem::Status::ToFetch});
          }
      }
      return ret;
    },
    [](UrlElem* url_elem, UrlElem::Status s) {
      if (s == UrlElem::Status::Done) {
        url_elem->status = UrlElem::Status::Done;
      }
    })->SetIter(2)->SetName("crawler main logic")->SetStaleness(2);
  Context::foreach(url_table, [](const UrlElem& url_elem) {
    LOG(INFO) << RED(url_elem.DebugString());
  }, "print all status");
  Context::count(url_table);

  Runner::Run();
  // Runner::PrintDag();
}
