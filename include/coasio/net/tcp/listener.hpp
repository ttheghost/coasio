#ifndef COASIO_NET_TCP_LISTENER_HPP
#define COASIO_NET_TCP_LISTENER_HPP
#include <asio/ip/tcp.hpp>

#include "endpoint.hpp"
#include "socket.hpp"

namespace coasio::net::tcp {
class listener {
public:
  using wait_type = asio::ip::tcp::acceptor::wait_type;

  static listener create() {
    asio::ip::tcp::acceptor lsnr(runtime::get_current_io_context());
    return listener{std::move(lsnr)};
  }

  static std::expected<listener, std::error_code>
  bind(endpoint ep, bool reuse_address = true) {
    auto lsnr = create();
    asio::error_code ec;

    lsnr.asio_handle().open(ep.asio_endpoint().protocol(), ec);
    if (ec)
      return std::unexpected{ec};

    if (reuse_address) {
      lsnr.asio_handle().set_option(
          asio::ip::tcp::acceptor::reuse_address(true), ec);
      if (ec)
        return std::unexpected{ec};
    }

    lsnr.asio_handle().bind(ep.asio_endpoint(), ec);
    if (ec)
      return std::unexpected{ec};
    return lsnr;
  }

  auto accept() {
    struct listen_awaiter {
      asio::error_code ec_;
      listener &listener_;
      std::optional<socket> socket_;

      explicit listen_awaiter(listener &listener)
          : listener_{listener}, socket_{std::nullopt} {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        runtime *rt = runtime::current();
        listener_.asio_handle().async_accept(
            [h, rt, this](const asio::error_code &ec,
                          asio::ip::tcp::socket sock) {
              ec_ = ec;
              if (!ec_) {
                socket_ = socket{(std::move(sock))};
              }
              rt->schedule(h);
            });
      }

      std::expected<socket, std::error_code> await_resume() noexcept {
        if (ec_)
          return std::unexpected{ec_};
        return std::move(socket_.value());
      }
    };

    return listen_awaiter{*this};
  }

  std::expected<void, std::error_code>
  listen(const int backlog = asio::socket_base::max_listen_connections) {
    asio::error_code ec;
    acceptor_.listen(backlog, ec);
    if (ec)
      return std::unexpected{ec};
    return {};
  }

  auto wait(wait_type type) {
    struct wait_awaiter {
      std::error_code ec_;
      listener &listener_;
      wait_type type_;

      explicit wait_awaiter(listener &listener, wait_type type)
          : listener_{listener}, type_{type} {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        runtime *rt = runtime::current();
        listener_.asio_handle().async_wait(
            type_, [h, rt, this](const asio::error_code &ec) {
              ec_ = ec;
              rt->schedule(h);
            });
      }

      std::expected<void, std::error_code> await_resume() noexcept {
        if (ec_)
          return std::unexpected(std::move(ec_));
        return {};
      }
    };

    return wait_awaiter{*this, type};
  }

  std::expected<void, std::error_code> close() {
    std::error_code ec_;
    acceptor_.close(ec_);
    if (ec_)
      return std::unexpected(std::move(ec_));
    return {};
  }

  bool is_open() const { return acceptor_.is_open(); }

  std::expected<endpoint, std::error_code> local_endpoint() const noexcept {
    asio::error_code ec;
    auto ep = acceptor_.local_endpoint(ec);
    if (ec)
      return std::unexpected{ec};
    return endpoint{ep};
  }

  auto native_handle() noexcept { return acceptor_.native_handle(); }

  asio::ip::tcp::acceptor &asio_handle() noexcept { return acceptor_; }

private:
  explicit listener(asio::ip::tcp::acceptor acceptor)
      : acceptor_(std::move(acceptor)) {}
  asio::ip::tcp::acceptor acceptor_;
};
} // namespace coasio::net::tcp

#endif // !COASIO_NET_TCP_LISTENER_HPP
