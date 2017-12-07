# xyz

[![Build Status](https://travis-ci.com/Yuzhen11/xyz.svg?token=q177tztPAL6tTAyRzSkG&branch=master)](https://travis-ci.com/Yuzhen11/xyz)

## Build

Download the source code.
```sh
git clone https://github.com/Yuzhen11/xyz.git
```

Go to the project root and do an out-of-source build using CMake:
```sh
mkdir debug
cd debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
make help               # List all build target
make $ApplicationName   # Build application
make -j                 # Build all applications with all threads

GLOG_logtostderr=1 ./HuskyUnitTest  # Show the log information
GLOG_logtostderr=1 ./HuskyUnitTest --gtest_filter=TestEngine.*  # Show the log information for particular test
```

To generate core dumped debug file:

```
ulimit -c unlimited
```

To output the debug message:

```
GLOG_logtostderr=1 GLOG_v=1 ./HuskyUnitTest --gtest_filter=TestMailbox*:
```
