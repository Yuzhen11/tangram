# Tangram

[![Build Status](https://travis-ci.org/Yuzhen11/tangram.svg?branch=master)](https://travis-ci.org/Yuzhen11/tangram)

*Tangram* is a distributed data analytics framework which enjoys the benefits from both the immutable and mutable data abstractions. 

Data analytics frameworks that adopt immutable data abstraction usually provide better support for _failure recovery_ and _straggler mitigation_, while those that adopt mutable data abstraction are more efficient for iterative workloads thanks to their support for _in-place state updates_ and _asynchronous execution_. Most existing frameworks adopt either one of the two data abstractions and do not enjoy the benefits of the other. 

Tangram adopts a novel programming model named **MapUpdate**, which can determine whether a distributed dataset is **mutable** or **immutable** in an application. MapUpdate not only offers good expressiveness, but also allows us to enjoy the benefits of both mutable and immutable abstractions. MapUpdate naturally supports iterative and asynchronous execution, and can use different recovery strategies adaptively according to failure scenarios. 

Tangram supports a variety of workloads including bulk processing, graph analytics, and iterative machine learning.

For more details about *Tangram*, please check our [Wiki](https://github.com/Yuzhen11/tangram/wiki).

For bugs in *Tangram*, please file an issue on [github issue platform](https://github.com/Yuzhen11/tangram/issues).

## Learn Tangram

* [Quick Start](https://github.com/Yuzhen11/tangram/wiki/Quick-Start)
* [Programming Guide](https://github.com/Yuzhen11/tangram/wiki/Programming-Guide)
* [API](https://github.com/Yuzhen11/tangram/wiki/API)
* [Applications](https://github.com/Yuzhen11/tangram/wiki/Applications)
* [Build](https://github.com/Yuzhen11/tangram/wiki/Build)

## Dependencies

Tangram has the following minimal dependencies:

* CMake (Version >= 3.0.2, if >= 3.6.0, it should set `CMAKE_PREFIX_PATH` first when occurring errors to find the following dependencies)
* ZeroMQ (including both [libzmq](https://github.com/zeromq/libzmq) and [cppzmq](https://github.com/zeromq/cppzmq))
* Boost (Version >= 1.58)
* A working C++ compiler (clang/gcc Version >= 4.9/icc/MSVC)
* TCMalloc (In [gperftools](https://github.com/gperftools/gperftools))
* [GLOG](https://github.com/google/glog) (Latest version, it will be included automatically)
* libhdfs3 [C/C++ HDFS Client](https://github.com/Pivotal-Data-Attic/pivotalrd-libhdfs3)

## Build

Download the source code.
```sh
git clone https://github.com/Yuzhen11/tangram.git
```

Go to the project root and do an out-of-source build using CMake:
```sh
mkdir debug
cd debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
make help               # List all build target
make $ApplicationName   # Build application
make SchedulerMain      # Build the Scheduler

make -j                 # Build all applications with all threads
```


## Run a Tangram Program

To run a Tangram program, users need to modify a Python launch script. Some examples can be found in [scripts/](https://github.com/Yuzhen11/tangram/tree/master/scripts). The launch script allows users to specify their binary, the hostnames and ports of the machines, command line arguments, etc. 

After that, running the program is as simple as:

```sh
python /path/to/your/script
```

To kill your Tangram program:

```sh
python /path/to/your/script kill
```

Run Tangram Unit Test
--------------------

Tangram provides a set unit tests (based on [gtest 1.7.0](https://github.com/google/googletest)) in `core/`. Run it with:

    $ make HuskyUnitTest  # yes, it is HuskyUnitTest
    $ ./HuskyUnitTest

Publication
---------------

Yuzhen Huang, Xiao Yan, Guanxian Jiang, Tatiana Jin, James Cheng, An Xu, Zhanhan Liu and Shuo Tu. **Tangram: Bridging Immutable and Mutable Abstractions for Distributed Data Analytics. (USENIX ATC '19)**. 

License
---------------

Copyright 2017-2019 Husky Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
