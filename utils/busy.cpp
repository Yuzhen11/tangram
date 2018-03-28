#include <iostream>
#include <thread>
#include <vector>
#include <cassert>
#include <cstdlib>

int main(int argc, char* argv[]) {
  assert(argc == 2);
  const int num_threads = atoi(argv[1]);
  assert(num_threads > 0);
  assert(num_threads <= 20);
  std::cout << "num threads: " << num_threads << std::endl;
  std::vector<std::thread> v;
  for (int i = 0; i < num_threads; ++ i) {
    v.push_back(std::thread([]() {
      while (1) { ; }
    }));
  }
  for (auto& th : v) {
    th.join();
  }
}
