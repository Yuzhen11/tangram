
namespace xyz {

void Loader::Process(Message msg) {
  SArrayBinStream ctrl_bin, bin;
  CHECK_EQ(msg.data.size(), 2);
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  int a;
  ctrl_bin >> a;
  if (a) {
    FetchBlock(bin);
  } else {
  }
}

void Loader::FetchBlock(SArrayBinStream bin) {
  std::string hdfs_namenode;
  int hdfs_namenode_port;
  std::string url;
  size_t offset;
  bin >> hdfs_namenode >> hdfs_namenode_port >> url >> offset;
  Init();
  Load();
}

void Loader::Init(std::string hdfs_namenode, std::string hdfs_namenode_port, std::string url) {
  struct hdfsBuilder* builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, hdfs_namenode);
  hdfsBuilderSetNameNodePort(builder, hdfs_namenode_port);
  fs_ = hdfsBuilderConnect(builder);
  hdfsFreeBuilder(builder);
  init_blocksize(fs_, url);
  data_ = new char[hdfs_block_size_];
}

void Loader::init_blocksize(hdfsFS fs, const std::string& url) {
  int num_files;
  hdfsFileInfo* file_info = hdfsListDirectory(fs, url.c_str(), &num_files);
  for (int i = 0; i < num_files; ++i) {
    if (file_info[i].mKind == kObjectKindFile) {
      hdfs_block_size = file_info[i].mBlockSize;
      hdfsFreeFileInfo(file_info, num_files);
      return;
    }
    continue;
  }
  LOG(ERROR) << "Block size init error. (File NOT exist or EMPTY directory)";
}

void Loader::Load() {
  boost::string_ref s;
  while (next(s)) {
    v.push_back(s.to_string());
  }
}

  bool next(boost::string_ref& ref) {
    if (buffer_.size() == 0) {
      bool success = fetch_new_block();
      if (success == false)
        return false;
    }
    // last charater in block
    if (r == buffer_.size() - 1) {
      // fetch next block
      buffer_ = fetch_block(true);
      if (buffer_.empty()) {
        // end of a file
        bool success = fetch_new_block();
        if (success == false)
          return false;
      } else {
        // directly process the remaing
        last_part_ = "";
        handle_next_block();
        ref = last_part_;
        return true;
      }
    }
    if (offset_ == 0 && r == 0) {
      // begin of a file
      l = 0;
      if (buffer_[0] == '\n')
        // for the case file starting with '\n'
        l = 1;
      r = find_next(buffer_, l, '\n');
    } else {
      // what if not found
      l = find_next(buffer_, r, '\n') + 1;
      r = find_next(buffer_, l, '\n');
    }
    // if the right end does not exist in current block
    if (r == boost::string_ref::npos) {
      auto last = buffer_.substr(l);
      last_part_ = std::string(last.data(), last.size());
      // fetch next subBlock
      buffer_ = fetch_block(true);
      handle_next_block();
      ref = last_part_;
      return true;
    } else {
      ref = buffer_.substr(l, r - l);
      return true;
    }
  }

  size_t find_next(boost::string_ref sref, size_t l, char c) {
    size_t r = l;
    while (r != sref.size() && sref[r] != c)
      r++;
    if (r == sref.size())
      return boost::string_ref::npos;
    return r;
  }

  void handle_next_block() {
    while (true) {
      if (buffer_.empty())
        return;

      r = find_next(buffer_, 0, '\n');
      if (r == boost::string_ref::npos) {
        // fetch next subBlock
        last_part_ += std::string(buffer_.data(), buffer_.size());
        buffer_ = fetch_block(true);
        continue;
      } else {
        last_part_ += std::string(buffer_.substr(0, r).data(), r);
        // TODO
        // clear_buffer();
        return;
      }
    }
  }

  bool fetch_new_block() {
    // fetch a new block
    buffer_ = fetch_block(false);
    if (buffer_.empty())
      //  no more files, exit
      return false;
    l = r = 0;
    return true;
  }

int Loader::read_block(const std::string& fn) {
  file_ = hdfsOpenFile(fs_, fn.c_str(), O_RDONLY, 0, 0, 0);
  CHECK(file_ != NULL) << "Hadoop file ile open fails";
  hdfsSeek(fs_, file_, offset_);
  size_t start = 0;
  size_t nbytes = 0;
  while (start < hdfs_block_size) {
    // only 128KB per hdfsRead
    nbytes = hdfsRead(fs_, file_, data_ + start, hdfs_block_size);
    start += nbytes;
    if (nbytes == 0)
      break;
  }
  return start;
}

boost::string_ref Loader::fetch_block(bool is_next = false) {
  int nbytes = 0;

  if (is_next) {
    nbytes = hdfsRead(fs_, file_, data_, hdfs_block_size);
    if (nbytes == 0)
      return "";
    if (nbytes == -1) {
      LOG(ERROR) << "read next block error!";
    }
  } else {

    answer >> fn;
    answer >> offset_;

    if (fn.empty()) {
      // no more files
      return "";
    }

    if (file_ != NULL) {
      int rc = hdfsCloseFile(fs_, file_);
      CHECK(rc == 0) << "close file fails";
      // Notice that "file" will be deleted inside hdfsCloseFile
      file_ = NULL;
    }

    // read block
    nbytes = read_block(fn);
  }
  return boost::string_ref(data_, nbytes);
}

}  // namespace xyz
