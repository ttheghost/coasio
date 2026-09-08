#ifndef COASIO_NET_TCP_SOCKET_HPP
#define COASIO_NET_TCP_SOCKET_HPP
#include <expected>
#include <string>
#include <string_view>

#include <asio/connect.hpp>
#include <asio/ip/tcp.hpp>

#include "coasio/net/helper.hpp"
#include "endpoint.hpp"
#include "resolver.hpp"

namespace coasio::net::tcp {
class tcpSocket {
  auto connect_impl(const asio::ip::tcp::resolver::results_type &endpoints) {
    struct connect_any_awaiter {
      std::error_code ec_;
      tcpSocket &socket_;
      asio::ip::tcp::resolver::results_type endpoints_;

      explicit connect_any_awaiter(
          tcpSocket &socket, const asio::ip::tcp::resolver::results_type &eps)
          : socket_{socket}, endpoints_{eps} {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        auto *rt = runtime::current();
        asio::async_connect(
            socket_.asio_handle(), endpoints_.begin(), endpoints_.end(),
            [h, rt, this](const asio::error_code &ec, auto /*iterator*/) {
              ec_ = ec;
              rt->schedule(h);
            });
      }

      std::expected<void, std::error_code> await_resume() const noexcept {
        if (ec_)
          return std::unexpected(ec_);
        return {};
      }
    };

    return connect_any_awaiter{*this, endpoints};
  }

public:
  static tcpSocket create() noexcept {
    asio::ip::tcp::socket socket(runtime::get_current_io_context());
    return tcpSocket{std::move(socket)};
  }

  static task<std::expected<tcpSocket, std::error_code>>
  connect_to(std::string_view host_and_port) {
    auto maybe_hap = parse_host_port(host_and_port);
    if (!maybe_hap) {
      co_return std::unexpected(maybe_hap.error());
    }
    if (!maybe_hap.value().port.has_value()) {
      co_return std::unexpected(
          std::make_error_code(std::errc::invalid_argument));
    }
    auto [host, port] = maybe_hap.value();
    std::cout << host << "<:>" << port.value() << "\n";

    resolver r = resolver::create();
    auto maybe_eps = co_await r.resolve(host, std::to_string(port.value()));
    if (!maybe_eps) {
      co_return std::unexpected(maybe_eps.error());
    }

    auto socket = tcpSocket::create();
    auto result = co_await socket.connect_impl(*maybe_eps);
    if (!result) {
      co_return std::unexpected(result.error());
    }
    co_return socket;
  }

  auto connect(const endpoint &ep) {
    struct connect_awaiter {
      std::error_code ec_;
      tcpSocket &socket_;
      endpoint ep_;

      explicit connect_awaiter(tcpSocket &socket, endpoint ep)
          : socket_{socket}, ep_{ep} {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        auto *rt = runtime::current();
        socket_.socket_.async_connect(
            ep_.asio_endpoint(), [h, rt, this](const asio::error_code &ec) {
              ec_ = ec;
              rt->schedule(h);
            });
      }

      std::expected<void, std::error_code> await_resume() const noexcept {
        if (ec_)
          return std::unexpected(std::move(ec_));
        return {};
      }
    };

    return connect_awaiter{*this, ep};
  }

  auto native_handle() noexcept { return socket_.native_handle(); }

  asio::ip::tcp::socket &asio_handle() noexcept { return socket_; }

private:
  explicit tcpSocket(asio::ip::tcp::socket socket) noexcept
      : socket_{std::move(socket)} {}

  asio::ip::tcp::socket socket_;
};
}; // namespace coasio::net::tcp

#endif // !COASIO_NET_TCP_SOCKET_HPP
