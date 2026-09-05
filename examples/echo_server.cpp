#include <iostream>
#include <chrono>
#include <coasio/runtime.hpp>
#include <coasio/task.hpp>
#include <coasio/time.hpp>

coasio::task<int> compute(int id) {
  std::cout << "start " << id
      << " thread=" << std::this_thread::get_id() << '\n';

  co_await coasio::time::sleep(std::chrono::seconds(1));

  std::cout << "end " << id
      << " thread=" << std::this_thread::get_id() << '\n';

  co_return id;
}

coasio::task<int> m() {
  co_return co_await compute(1) + co_await compute(2);
}

coasio::task<int> loop() {
  coasio::runtime::spawn(compute(3));
  coasio::runtime::spawn([]() -> coasio::task<int> {
    std::cout << co_await m() << "\n";
    co_return 0;
  }());
  while (true) {
  }
  co_return 0;
}

int main() {
  coasio::runtime rt;
  rt.block_on(loop().handle());
}
