#include "coasio/time.hpp"
#include <chrono>
#include <coasio/runtime.hpp>
#include <coasio/task.hpp>
#include <iostream>

#define COASIO_MAIN$()                                                         \
  auto __user_main() -> ::coasio::task<int>;                                   \
  int main() {                                                                 \
    ::coasio::runtime rt;                                                      \
    rt.block_on(__user_main());                                                \
  }                                                                            \
  auto __user_main() -> ::coasio::task<int>

coasio::task<int> long_running_work() {
  std::cout << "work: starting, will sleep 5s\n";

  auto result = co_await coasio::time::sleep(std::chrono::seconds(5));

  if (!result) {
    std::cout << "work: cancelled (" << result.error().message() << ")\n";
    co_return -1;
  }

  std::cout << "work: finished normally\n";
  co_return 42;
}

COASIO_MAIN$() {
  auto handle = coasio::runtime::spawn(long_running_work());

  co_await coasio::time::sleep(std::chrono::seconds(1));
  std::cout << "runner: aborting the work task now\n";
  handle.abort();

  co_await coasio::time::sleep(std::chrono::milliseconds(200));

  co_return 0;
}
