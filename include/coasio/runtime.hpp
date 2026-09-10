#ifndef COASIO_RUNTIME_HPP
#define COASIO_RUNTIME_HPP

#include <asio/io_context.hpp>
#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "task.hpp"

namespace coasio {
class runtime;

class worker {
  runtime *runtime_;

public:
  explicit worker(runtime *runtime) : runtime_(runtime) {}

  void run() const;
};

class io_worker {
  runtime *runtime_;

public:
  explicit io_worker(runtime *runtime) : runtime_(runtime) {}

  void run() const;
};

class runtime {
  std::queue<std::coroutine_handle<>> global_tasks_;
  std::mutex global_tasks_queue_mutex_;
  std::condition_variable global_tasks_queue_cv_;
  std::atomic<bool> stop_requested_{false};
  asio::io_context io_context_;
  asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
  std::vector<std::jthread> io_worker_threads_;
  std::vector<std::jthread> worker_threads_;

  static inline thread_local runtime *current_runtime_ = nullptr;

public:
  runtime();

  ~runtime();

  static runtime *current() noexcept { return current_runtime_; }

  struct context_guard {
    runtime *prev_;

    explicit context_guard(runtime *rt) noexcept : prev_(current_runtime_) {
      current_runtime_ = rt;
    }

    ~context_guard() noexcept { current_runtime_ = prev_; }
  };

  template <typename T> T block_on(task<T> t) {
    context_guard guard(this);

    using promise_t = std::conditional_t<std::is_void_v<T>, std::promise<void>,
                                         std::promise<T>>;

    auto promise = std::make_shared<promise_t>();
    auto future = promise->get_future();

    spawn([](task<T> t, std::shared_ptr<promise_t> p) -> task<void> {
      try {
        if constexpr (std::is_void_v<T>) {
          co_await std::move(t);
          p->set_value();
        } else {
          T result = co_await std::move(t);
          p->set_value(std::move(result));
        }
      } catch (...) {
        p->set_exception(std::current_exception());
      }
    }(std::move(t), promise));

    return future.get();
  }

  template <typename T> static void spawn(task<T> task) {
    runtime *rt = current();
    if (!rt) {
      std::cout << "Called outside a coasio runtime\n";
      std::terminate();
    }
    if (auto handle = task.detach())
      rt->schedule(handle);
  }

  asio::io_context &get_io_context() noexcept { return io_context_; }

  static asio::io_context &get_current_io_context() noexcept {
    auto *rt = current();
    if (!rt) {
      std::cout << "Called outside a coasio runtime\n";
      std::terminate();
    }
    return rt->get_io_context();
  }

  void schedule(std::coroutine_handle<> h) {
    if (!h)
      return;
    put_task_in_queue(h);
  }

  // Queue
  std::optional<std::coroutine_handle<>> get_next_task_from_queue() {
    std::unique_lock lock(global_tasks_queue_mutex_);
    global_tasks_queue_cv_.wait(
        lock, [this] { return !global_tasks_.empty() || stop_requested_; });
    if (stop_requested_ && global_tasks_.empty()) {
      return std::nullopt;
    }
    auto h = global_tasks_.front();
    global_tasks_.pop();
    return h;
  }

  void put_task_in_queue(std::coroutine_handle<> h) {
    std::unique_lock lock(global_tasks_queue_mutex_);
    global_tasks_.push(h);
    global_tasks_queue_cv_.notify_one();
  }

  friend class worker;
  friend class io_worker;
};
}; // namespace coasio

#endif // !COASIO_RUNTIME_HPP
