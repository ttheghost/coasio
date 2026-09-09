#include <coasio/runtime.hpp>

#include <print>

void coasio::worker::run() const {
  std::print("worker thread id: {}\n", std::this_thread::get_id());
  runtime::context_guard guard(runtime_);

  while (!runtime_->stop_requested_) {
    if (auto task = runtime_->get_next_task_from_queue()) {
      if (*task) {
        task->resume();
      }
    }
  }
}

void coasio::io_worker::run() const {
  std::print("io_worker thread id: {}\n", std::this_thread::get_id());
  runtime_->io_context_.run();
}

coasio::runtime::runtime() : work_guard_(asio::make_work_guard(io_context_)) {
  auto num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0)
    num_threads = 1;

  std::print("num_threads: {}\n", num_threads);

  worker_threads_.reserve(num_threads);
  for (unsigned int i = 0; i < num_threads; ++i) {
    worker_threads_.emplace_back([this]() {
      const worker w(this);
      w.run();
    });
  }

  unsigned int num_io_threads = 2; // TODO: configurable
  io_worker_threads_.reserve(num_io_threads);
  for (unsigned int i = 0; i < num_io_threads; ++i) {
    io_worker_threads_.emplace_back([this]() {
      const io_worker w(this);
      w.run();
    });
  }
}

coasio::runtime::~runtime() {
  stop_requested_ = true;
  global_tasks_queue_cv_.notify_all();
  work_guard_.reset();
  io_context_.stop();

  io_worker_threads_.clear();
  worker_threads_.clear();
};
