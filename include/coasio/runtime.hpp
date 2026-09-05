#ifndef COASIO_RUNTIME_HPP
#define COASIO_RUNTIME_HPP

#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <coroutine>
#include <iostream>
#include <asio/io_context.hpp>

#include "task.hpp"

namespace coasio {
  class runtime;

  class worker {
    runtime *runtime_;

  public:
    explicit worker(runtime *runtime) : runtime_(runtime) {
    }

    void run() const;
  };

  class io_worker {
    runtime *runtime_;

  public:
    explicit io_worker(runtime *runtime) : runtime_(runtime) {
    }

    void run() const;
  };

  class runtime {
    std::queue<std::coroutine_handle<> > global_tasks_;
    std::mutex global_tasks_queue_mutex_;
    std::condition_variable global_tasks_queue_cv_;
    std::vector<std::jthread> worker_threads_;
    std::vector<std::jthread> io_worker_threads_;
    asio::io_context io_context_;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
    std::atomic<bool> stop_requested_{false};

    static inline thread_local runtime *current_runtime_ = nullptr;

  public:
    runtime();
    ~runtime();

    static runtime *current() noexcept {
      return current_runtime_;
    }

    struct context_guard {
      runtime *prev_;

      explicit context_guard(runtime *rt) noexcept : prev_(current_runtime_) {
        current_runtime_ = rt;
      }

      ~context_guard() noexcept {
        current_runtime_ = prev_;
      }
    };

    void block_on(std::coroutine_handle<> task);

    template<typename T>
    static void spawn(task<T> task) {
      runtime *rt = current();
      if (!rt) {
        std::cout << "Called outside a coasio runtime\n";
        std::terminate();
      }
      rt->schedule(task.release());
    }

    asio::io_context& get_io_context() {
      return io_context_;
    }

    void schedule(std::coroutine_handle<> h) {
      if (!h) return;
      std::unique_lock lock(global_tasks_queue_mutex_);
      global_tasks_.push(h);
      global_tasks_queue_cv_.notify_one();
    }

    friend class worker;
    friend class io_worker;
  };
};

#endif // !COASIO_RUNTIME_HPP
