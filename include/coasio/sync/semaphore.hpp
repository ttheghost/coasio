#ifndef COASIO_SYNC_SEMAPHORE_HPP
#define COASIO_SYNC_SEMAPHORE_HPP

#include <atomic>
#include <cassert>
#include <coroutine>
#include <expected>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <utility>

#include "coasio/runtime.hpp"

namespace coasio::sync {
namespace detail {
struct waiter {
  waiter *prev{nullptr};
  waiter *next{nullptr};
  std::coroutine_handle<> handle{};
  runtime *rt{nullptr};
  size_t requested_permits{1};
  bool acquired{false};
};

class semaphore_internal {
  struct to_wake {
    runtime *rt{};
    std::coroutine_handle<> h;
  };

  std::atomic<ptrdiff_t> permits_;
  std::atomic<bool> has_waiters_;
  std::mutex queue_mtx_;
  waiter *head_{nullptr};
  waiter *tail_{nullptr};

  void link_tail(waiter *w) noexcept {
    w->next = nullptr;
    w->prev = tail_;
    if (tail_)
      tail_->next = w;
    else
      head_ = w;
    tail_ = w;
    has_waiters_.store(true, std::memory_order_seq_cst);
  }

  void unlink(waiter *w) noexcept {
    if (w->prev)
      w->prev->next = w->next;
    else
      head_ = w->next;
    if (w->next)
      w->next->prev = w->prev;
    else
      tail_ = w->prev;
    w->prev = w->next = nullptr;
    if (head_ == nullptr) {
      has_waiters_.store(false, std::memory_order_seq_cst);
    }
  }

  // caller holds queue_mtx_
  size_t grant_locked(to_wake *out, const size_t cap,
                      const waiter *self = nullptr) {
    size_t n = 0;
    while (head_) {
      const auto need = static_cast<ptrdiff_t>(head_->requested_permits);
      auto cur = permits_.load(std::memory_order_seq_cst);
      if (cur < need)
        break;
      if (!permits_.compare_exchange_weak(cur, cur - need,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed))
        continue;
      waiter *w = head_;
      unlink(w);
      w->acquired = true;
      if (w == self)
        continue; // caller will just not suspend
      if (n < cap)
        out[n++] = {w->rt, w->handle};
      else
        w->rt->schedule(w->handle);
    }
    return n;
  }

public:
  static constexpr size_t MAX_PERMITS = std::numeric_limits<ptrdiff_t>::max();

  explicit semaphore_internal(const size_t initial_permits) noexcept
      : permits_(static_cast<ptrdiff_t>(initial_permits)), has_waiters_(false) {
    assert(initial_permits <= MAX_PERMITS &&
           "Semaphore permit count exceeded MAX_PERMITS");
  }

  semaphore_internal(semaphore_internal &other) = delete;
  semaphore_internal &operator=(semaphore_internal &other) = delete;

  [[nodiscard]] bool try_acquire(const size_t permits = 1) noexcept {
    if (permits == 0)
      return true;

    if (has_waiters_.load(std::memory_order_seq_cst)) {
      return false;
    }

    const auto need = static_cast<ptrdiff_t>(permits);
    auto current = permits_.load(std::memory_order_seq_cst);
    while (current >= need) {
      if (permits_.compare_exchange_weak(current, current - need,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  // true if acquired, false if enqueued;
  // we assume a waiter handle and rt are populated
  [[nodiscard]] bool try_acquire_or_enqueue(waiter *w) {
    to_wake buf[16];
    bool acquired = false;
    size_t n;
    {
      std::lock_guard lock(queue_mtx_);
      link_tail(w);
      n = grant_locked(buf, 16, w);
      acquired = w->acquired;
    }
    for (size_t i = 0; i < n; ++i)
      buf[i].rt->schedule(buf[i].h);
    return acquired;
  }

  void dequeue(waiter *w) noexcept {
    to_wake buf[16];
    size_t n = 0;
    {
      std::lock_guard lock(queue_mtx_);
      if (!w->acquired) {
        unlink(w);
      } else {
        permits_.fetch_add(static_cast<ptrdiff_t>(w->requested_permits),
                           std::memory_order_seq_cst);
      }
      n = grant_locked(buf, 16);
    }
    for (size_t i = 0; i < n; ++i)
      buf[i].rt->schedule(buf[i].h);
  }

  void release(const size_t permits = 1) noexcept {
    if (permits == 0)
      return;
    permits_.fetch_add(static_cast<ptrdiff_t>(permits),
                       std::memory_order_seq_cst);
    if (!has_waiters_.load(std::memory_order_seq_cst))
      return;

    to_wake buf[16];
    size_t n;
    {
      std::lock_guard lock(queue_mtx_);
      n = grant_locked(buf, 16);
    }
    for (size_t i = 0; i < n; ++i)
      buf[i].rt->schedule(buf[i].h);
  }

  size_t available_permits() const noexcept {
    return permits_.load(std::memory_order_seq_cst);
  }
};
} // namespace detail

class semaphore_permit {
public:
  semaphore_permit(const std::shared_ptr<detail::semaphore_internal> &sem,
                   const size_t count) noexcept
      : sem_(sem), permits_(count) {}

  ~semaphore_permit() {
    if (auto sem = sem_.lock(); sem && permits_ > 0) {
      sem->release(permits_);
    }
  }

  semaphore_permit(semaphore_permit &&other) noexcept
      : sem_(std::move(other.sem_)),
        permits_(std::exchange(other.permits_, 0)) {}
  semaphore_permit &operator=(semaphore_permit &&other) noexcept {
    if (this != &other) {
      if (auto sem = sem_.lock(); sem && permits_ > 0) {
        sem->release(permits_);
      }
      sem_ = std::move(other.sem_);
      permits_ = std::exchange(other.permits_, 0);
    }
    return *this;
  }

  semaphore_permit(const semaphore_permit &other) = delete;
  semaphore_permit &operator=(const semaphore_permit &other) = delete;

  void forget() noexcept { permits_ = 0; }

  void merge(semaphore_permit &other) {
    assert(!sem_.owner_before(other.sem_) && !other.sem_.owner_before(sem_) &&
           "Cannot merge permits from different semaphores");
    permits_ += other.permits_;
    other.forget();
  }

  size_t count() const noexcept { return permits_; }

private:
  std::weak_ptr<detail::semaphore_internal> sem_;
  size_t permits_{};
};

class semaphore {
public:
  static constexpr size_t MAX_PERMITS = detail::semaphore_internal::MAX_PERMITS;

  explicit semaphore(const size_t initial_permits) noexcept
      : sem_(std::make_shared<detail::semaphore_internal>(initial_permits)) {}

  std::optional<semaphore_permit>
  try_acquire(const size_t permits = 1) noexcept {
    assert(permits <= MAX_PERMITS &&
           "Semaphore permit count exceeded MAX_PERMITS");

    if (sem_->try_acquire(permits)) {
      return semaphore_permit{sem_, permits};
    }
    return std::nullopt;
  }

  auto acquire(const size_t permits = 1) noexcept {
    assert(permits <= MAX_PERMITS &&
           "Semaphore permit count exceeded MAX_PERMITS");

    struct acquire_awaiter {
      semaphore sem_;
      size_t requested_;
      detail::waiter node_{};
      bool enqueued_{false};

      acquire_awaiter(const semaphore &s, const size_t n) noexcept
          : sem_(s), requested_(n) {
        node_.requested_permits = n;
      }

      ~acquire_awaiter() {
        if (enqueued_) {
          sem_.sem_->dequeue(&node_);
        }
      }

      bool await_ready() noexcept {
        if (requested_ == 0 || sem_.sem_->try_acquire(requested_)) {
          node_.acquired = true;
          return true;
        }
        return false;
      }

      bool await_suspend(std::coroutine_handle<> h) noexcept {
        node_.handle = h;
        node_.rt = runtime::current();
        enqueued_ = true;
        if (sem_.sem_->try_acquire_or_enqueue(&node_)) {
          enqueued_ = false;
          return false;
        }
        // NOTE: there is a chance that another thread may already have woken
        // and resumed (or destroyed) the coroutine Touching `this` is UB
        return true;
      }

      std::expected<semaphore_permit, std::error_code> await_resume() noexcept {
        if (node_.acquired) {
          enqueued_ = false;
          return semaphore_permit{sem_.sem_, requested_};
        }
        // for future cancellation support
        return std::unexpected(std::error_code{});
      }
    };

    return acquire_awaiter{*this, permits};
  }

  void release(const size_t permits = 1) noexcept {
    if (permits == 0)
      return;
    sem_->release(permits);
  }

  size_t available_permits() const noexcept {
    return sem_->available_permits();
  }

private:
  std::shared_ptr<detail::semaphore_internal> sem_;
};
} // namespace coasio::sync

#endif // !COASIO_SYNC_SEMAPHORE_HPP
