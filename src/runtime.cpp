#include <coasio/runtime.hpp>

#include <iostream>

void coasio::worker::run() const {
  runtime::context_guard guard(runtime_);

  while (!runtime_->stop_requested_) {
    std::coroutine_handle<> task; {
      std::unique_lock lock(runtime_->global_tasks_queue_mutex_);

      runtime_->global_tasks_queue_cv_.wait(lock, [this] {
        return !runtime_->global_tasks_.empty() || runtime_->stop_requested_;
      });

      if (runtime_->stop_requested_ && runtime_->global_tasks_.empty()) {
        return;
      }

      task = runtime_->global_tasks_.front();
      runtime_->global_tasks_.pop();
    }
    if (task) {
      task.resume();
      //if (task.done())
      //  task.destroy();
    }
  }
}

void coasio::io_worker::run() const {
  runtime_->io_context_.run();
}

coasio::runtime::runtime(): work_guard_(asio::make_work_guard(io_context_)) {
  auto num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0)
    num_threads = 1;

  std::cout << "num_threads: " << num_threads << "\n";

  worker_threads_.reserve(num_threads);
  for (unsigned int i = 0; i < num_threads; ++i) {
    worker_threads_.emplace_back([this]() {
      const worker w(this);
      w.run();
    });
  }

  io_worker_threads_.reserve(1); // TODO: configurable
  io_worker_threads_.emplace_back([this]() {
    const io_worker w(this);
    w.run();
  });
}

coasio::runtime::~runtime() {
  stop_requested_ = true;
  global_tasks_queue_cv_.notify_all();
  work_guard_.reset();
  io_context_.stop();
};

void coasio::runtime::block_on(const std::coroutine_handle<> task) {
  context_guard guard(this);

  if (!task) return;
  while (task.done() == false) {
    task.resume();
  }
}
