#include "base/color.hpp"
#include "core/plan/runner.hpp"
#include "boost/tokenizer.hpp"

#include <string>
#include <cmath>
#include <regex>
#include <boost/algorithm/string.hpp>    

DEFINE_int32(num_of_docs, 1, "# number of docs");
DEFINE_int32(num_doc_partition, 10, "");
DEFINE_int32(num_term_partition, 10, "");

DEFINE_string(url, "", "The url for hdfs file");


using namespace xyz;

class Document {
   public:
    using KeyT = std::string;
    Document() = default;
    explicit Document(const KeyT& t) : title(t) {}
    KeyT title;
    std::vector<float> tf;
    std::vector<float> tf_idf;
    std::vector<size_t> words;
    int total_words = 0;
    const KeyT& id() const { return title; }
    KeyT Key() const { return title; }
    friend SArrayBinStream &operator<<(SArrayBinStream& stream, const Document& doc) {
        stream << doc.title << doc.tf << doc.tf_idf << doc.words << doc.total_words;
        return stream;
    }
    friend SArrayBinStream &operator>>(SArrayBinStream& stream, Document& doc) {
        stream >> doc.title >> doc.tf >> doc.tf_idf >> doc.words >> doc.total_words;
        return stream;
    }
};

class Term {
   public:
    using KeyT = std::size_t;
    Term() = default;
    explicit Term(const KeyT& term) : termid(term) {}
    KeyT termid;
    int idf;
    const KeyT& id() const { return termid; }
    KeyT Key() const { return termid; }
    friend SArrayBinStream &operator<<(SArrayBinStream& stream, const Term& t) {
        stream << t.termid << t.idf;
        return stream;
    }
    friend SArrayBinStream &operator>>(SArrayBinStream& stream, Term& t) {
        stream >> t.termid >> t.idf;
        return stream;
    }
};



int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  // load and generate two collections
  auto loaded_docs = Context::load_wholefiles(FLAGS_url, [](std::string content) {
      //parse extract title
      std::regex rgx(".*?title=\"(.*?)\".*?");
      std::smatch match;
      std::string title;
      if (std::regex_search(content, match, rgx)){
        //LOG(INFO) << match[1];
        title = match[1];
      } else {
        CHECK(false) << "cannot match title";
      }
      
      Document doc(title);
      std::vector<int> count;
      if (content.size() > 0) {
        boost::char_separator<char> sep(" \t\n.,()\'\":;!?<>");
        boost::tokenizer<boost::char_separator<char>> tok(content, sep);
        for (auto& w : tok) {
            doc.words.push_back(std::hash<std::string>{}(w));
            //std::transform(doc.words.back().begin(), doc.words.back().end(), doc.words.back().begin(), ::tolower);
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
      }

      return doc;
  });

  // no need indexed_docs using pull model.
  // auto indexed_docs = Context::placeholder<Document>(FLAGS_num_doc_partition);
  auto terms_key_part_mapper = std::make_shared<HashKeyToPartMapper<size_t>>(FLAGS_num_term_partition);
  auto terms = Context::placeholder<Term>(FLAGS_num_term_partition, terms_key_part_mapper);
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
      [](TypedPartition<Document>* p, AbstractMapProgressTracker* t) {
        std::vector<std::pair<size_t, int>> ret;
        for (auto& doc : *p) {
          for(int i = 0; i < doc.words.size(); i++){
            ret.push_back({doc.words[i],1});
          }
        }
        return ret;
      },
      [](Term *term, int n) { 
        term->idf += n;

      })
      ->SetCombine([](int *a, int b){
        *a += b;
      })
      ->SetName("Out all the doc");

  // Context::count(terms);
  auto dummy_collection = Context::placeholder<CountObjT>(1);
  Context::mappartwithjoin(
      loaded_docs, terms, dummy_collection,
      [terms_key_part_mapper](TypedPartition<Document>* p, TypedCache<Term> *typed_cache, AbstractMapProgressTracker* t) {
        
        std::vector<std::pair<int, int>> ret;
        std::vector<std::shared_ptr<IndexedSeqPartition<Term>>> with_parts(FLAGS_num_term_partition);
        std::map<std::string, int> terms_map;
        int start_idx = rand()%FLAGS_num_term_partition;  // random start_idx to avoid overload on one point
        for (int i = 0; i < FLAGS_num_term_partition; i++) {
          int idx = (start_idx + i) % FLAGS_num_term_partition;
          auto start_time = std::chrono::system_clock::now();
          auto part = typed_cache->GetPartition(idx);
          auto end_time = std::chrono::system_clock::now();
          std::chrono::duration<double> duration = end_time - start_time;
          // if (FLAGS_node_id == 0) {
          //   LOG(INFO) << GREEN("fetch time for " + std::to_string(idx) << " : " + std::to_string(duration.count()));
          // }
          with_parts[idx] = std::dynamic_pointer_cast<IndexedSeqPartition<Term>>(part);
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
            doc.tf_idf[i] = doc.tf[i] * std::log(FLAGS_num_of_docs / float(count));
          }
        }
        */

        // method2
        for (auto& doc : *p) {
          for(int i = 0; i < doc.words.size(); i++){
            size_t term  = doc.words[i];
            auto* with_p = with_parts[terms_key_part_mapper->Get(term)];
            auto* t = with_p->Find(term);
            CHECK_NOTNULL(t);
            int count = t->idf;
            doc.tf_idf[i] = doc.tf[i] * std::log(FLAGS_num_of_docs / float(count));
          }
        }

        for (int i = 0; i < FLAGS_num_term_partition; i++) {
          typed_cache->ReleasePart(i);
        }
        return ret;
      },
      [](CountObjT* t, int) { 
      })
      ->SetName("Send idf back to doc");
  Runner::Run();
}
