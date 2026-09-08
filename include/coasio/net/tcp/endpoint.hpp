#ifndef COASIO_NET_TCP_ENDPOINT_HPP
#define COASIO_NET_TCP_ENDPOINT_HPP
#include <asio/ip/tcp.hpp>

#include "coasio/net/base_endpoint.hpp"

namespace coasio::net::tcp {
using endpoint = base_endpoint<asio::ip::tcp>;
};

#endif // !COASIO_NET_TCP_ENDPOINT_HPP
