#include <coasio/task.hpp>
#include <iostream>

coasio::task<int> compute() {
  co_yield 6;
  co_return 42;
}

int main() {
  try {
    auto t = compute();
    std::cout << "result: " << t.get() << "\n";
    t.resume();
    std::cout << "result: " << t.get() << "\n";
  } catch (std::exception &e) {
    std::cerr << e.what() << "\n";
  }
}
