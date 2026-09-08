#ifndef COASIO_NET_BASE_ENDPOINT_HPP
#define COASIO_NET_BASE_ENDPOINT_HPP
#include <asio/ip/tcp.hpp>

#include "tcp/ip_address.hpp"
#include "tcp/socket.hpp"

namespace coasio::net {
template <typename InternetProtocol> class base_endpoint {
public:
  typedef InternetProtocol protocol_type;

  explicit base_endpoint(
      asio::ip::basic_endpoint<protocol_type> endpoint) noexcept
      : endpoint_{endpoint} {}

  base_endpoint(tcp::ipAddress addr, const uint16_t port) noexcept
      : endpoint_{addr.asio_address(), port} {}

  tcp::ipAddress address() const noexcept {
    return tcp::ipAddress{endpoint_.address()};
  }

  uint16_t port() const noexcept { return endpoint_.port(); }

  void set_address(tcp::ipAddress &addr) noexcept {
    endpoint_.set_address(addr.asio_address());
  }

  void set_port(uint16_t port) noexcept { endpoint_.set_port(port); }

  std::ostream &operator<<(std::ostream &os) const { return os << endpoint_; }

  [[nodiscard]] auto asio_endpoint() const noexcept { return endpoint_; }

private:
  asio::ip::basic_endpoint<protocol_type> endpoint_;
};
}; // namespace coasio::net

#endif // !COASIO_NET_BASE_ENDPOINT_HPP
