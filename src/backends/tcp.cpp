// Built-in stream backend over TCP.
//
// Address in the config: "host" or "host:base_port". Each process listens on
//   port = base_port + app * inst_stride + inst
// (defaults: base_port 40000 from [backend.tcp], inst_stride 32), so several
// processes can share a host without a broker.
//
// Threads: one acceptor, plus one blocking reader per link. Good enough for
// tens of links; an epoll loop is a drop-in replacement behind the same SPI.

#include "pushseg/backend.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <thread>
#include <vector>

#include <netdb.h>
#include <unistd.h>

namespace pushseg::backends {
namespace {

using backend::Endpoint;
using backend::Link;
using backend::LinkHandler;
using backend::Options;

constexpr int kMaxIov = 8;
constexpr std::size_t kRecvBuf = 256 * 1024;

unsigned long opt_ul(const Options& o, const char* key, unsigned long def) {
  const auto it = o.find(key);
  return it == o.end() ? def : std::strtoul(it->second.c_str(), nullptr, 10);
}

// Splits "host[:base_port]".
bool parse_address(const std::string& addr,
                   unsigned long default_base,
                   std::string& host,
                   unsigned long& base) {
  const std::size_t colon = addr.rfind(':');
  if (colon == std::string::npos) {
    host = addr;
    base = default_base;
    return true;
  }
  host = addr.substr(0, colon);
  const std::string p = addr.substr(colon + 1);
  if (p.empty() || p.find_first_not_of("0123456789") != std::string::npos) {
    return false;
  }
  base = std::strtoul(p.c_str(), nullptr, 10);
  return true;
}

void tune(int fd) {
  int one = 1;
  (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  (void)::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
}

class TcpLink final : public Link {
public:
  explicit TcpLink(int fd) : fd_(fd) {}
  ~TcpLink() override {
    ::close(fd_);
  }
  TcpLink(const TcpLink&) = delete;
  TcpLink& operator=(const TcpLink&) = delete;

  Status sendv(const struct iovec* iov, int iovcnt) override {
    if (iovcnt < 0 || iovcnt > kMaxIov) {
      return Status::kInvalidArgument;
    }
    iovec v[kMaxIov];
    std::size_t cnt = 0;
    for (int i = 0; i < iovcnt; ++i) {
      if (iov[i].iov_len > 0) {
        v[cnt++] = iov[i];
      }
    }
    std::lock_guard<std::mutex> lk(mu_);
    std::size_t idx = 0;
    while (idx < cnt) {
      if (closed_.load(std::memory_order_acquire)) {
        return Status::kClosed;
      }
      msghdr msg{};
      msg.msg_iov = v + idx;
      msg.msg_iovlen = cnt - idx;
      const ssize_t n = ::sendmsg(fd_, &msg, MSG_NOSIGNAL);
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        return Status::kDisconnected;
      }
      auto left = static_cast<std::size_t>(n);
      while (idx < cnt && left >= v[idx].iov_len) {
        left -= v[idx].iov_len;
        ++idx;
      }
      if (idx < cnt && left > 0) {
        v[idx].iov_base = static_cast<char*>(v[idx].iov_base) + left;
        v[idx].iov_len -= left;
      }
    }
    return Status::kOk;
  }

  void close() override {
    if (!closed_.exchange(true)) {
      ::shutdown(fd_, SHUT_RDWR);
    }
  }

  int fd() const noexcept {
    return fd_;
  }
  bool closed() const noexcept {
    return closed_.load(std::memory_order_acquire);
  }

private:
  int fd_;
  std::mutex mu_;
  std::atomic<bool> closed_{false};
};

void reader_loop(const std::shared_ptr<TcpLink>& link, const std::shared_ptr<LinkHandler>& h) {
  std::vector<std::uint8_t> buf(kRecvBuf);
  Status why = Status::kDisconnected;
  while (true) {
    const ssize_t n = ::recv(link->fd(), buf.data(), buf.size(), 0);
    if (n > 0) {
      h->on_recv(buf.data(), static_cast<std::size_t>(n));
      continue;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (link->closed()) {
      why = Status::kClosed;
    } else {
      why = n == 0 ? Status::kDisconnected : Status::kIoError;
    }
    break;
  }
  link->close();
  h->on_down(why);
}

class TcpBackend final : public backend::StreamBackend {
public:
  ~TcpBackend() override {
    stop();
  }

  Status start(const Endpoint& self, const Options& opts, backend::StreamHost& host) override {
    host_ = &host;
    default_base_ = opt_ul(opts, "base_port", 40000);
    stride_ = opt_ul(opts, "inst_stride", 32);
    connect_timeout_ms_ = opt_ul(opts, "connect_timeout_ms", 1000);

    std::string bind_host;
    unsigned long base = 0;
    if (!parse_address(self.address, default_base_, bind_host, base)) {
      return Status::kInvalidArgument;
    }
    unsigned long port = 0;
    if (!port_for(base, self.app, self.inst, port)) {
      return Status::kInvalidArgument;
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    addrinfo* res = nullptr;
    const std::string port_s = std::to_string(port);
    if (::getaddrinfo(
            bind_host.empty() ? nullptr : bind_host.c_str(), port_s.c_str(), &hints, &res) != 0) {
      return Status::kInvalidArgument;
    }
    Status st = Status::kIoError;
    for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
      const int fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
      if (fd < 0) {
        continue;
      }
      int one = 1;
      (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
      if (::bind(fd, ai->ai_addr, ai->ai_addrlen) == 0 && ::listen(fd, 64) == 0) {
        listen_fd_ = fd;
        st = Status::kOk;
        break;
      }
      st = errno == EADDRINUSE ? Status::kExists : Status::kIoError;
      ::close(fd);
    }
    ::freeaddrinfo(res);
    if (st != Status::kOk) {
      return st;
    }
    acceptor_ = std::thread([this] { accept_loop(); });
    return Status::kOk;
  }

  Status connect(const Endpoint& remote,
                 std::shared_ptr<LinkHandler> handler,
                 std::shared_ptr<Link>& out) override {
    std::string host;
    unsigned long base = 0;
    unsigned long port = 0;
    if (!parse_address(remote.address, default_base_, host, base) || host.empty() ||
        !port_for(base, remote.app, remote.inst, port)) {
      return Status::kInvalidArgument;
    }
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    const std::string port_s = std::to_string(port);
    if (::getaddrinfo(host.c_str(), port_s.c_str(), &hints, &res) != 0) {
      return Status::kNotFound;
    }
    int fd = -1;
    for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
      fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
      if (fd < 0) {
        continue;
      }
      // Bounded connect: SO_SNDTIMEO also applies to connect() on Linux.
      timeval tv{};
      tv.tv_sec = static_cast<time_t>(connect_timeout_ms_ / 1000);
      tv.tv_usec = static_cast<suseconds_t>((connect_timeout_ms_ % 1000) * 1000);
      (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
      if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
        timeval zero{};
        (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &zero, sizeof(zero));
        break;
      }
      ::close(fd);
      fd = -1;
    }
    ::freeaddrinfo(res);
    if (fd < 0) {
      return Status::kNotFound;
    }
    tune(fd);
    auto link = std::make_shared<TcpLink>(fd);
    if (!spawn_reader(link, std::move(handler))) {
      return Status::kClosed;
    }
    out = link;
    return Status::kOk;
  }

  void stop() override {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (stopping_) {
        return;
      }
      stopping_ = true;
    }
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);  // wakes accept()
    }
    if (acceptor_.joinable()) {
      acceptor_.join();
    }
    if (listen_fd_ >= 0) {
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    std::vector<std::weak_ptr<TcpLink>> links;
    std::vector<std::thread> threads;
    {
      std::lock_guard<std::mutex> lk(mu_);
      links.swap(links_);
      threads.swap(threads_);
    }
    for (auto& w : links) {
      if (auto l = w.lock()) {
        l->close();
      }
    }
    for (auto& t : threads) {
      t.join();
    }
  }

private:
  bool port_for(unsigned long base, AppId app, InstId inst, unsigned long& port) const {
    if (inst >= stride_) {
      return false;
    }
    port = base + app * stride_ + inst;
    return port > 0 && port <= 65535;
  }

  void accept_loop() {
    while (true) {
      const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
      if (fd < 0) {
        {
          std::lock_guard<std::mutex> lk(mu_);
          if (stopping_) {
            return;
          }
        }
        if (errno == EINTR || errno == ECONNABORTED) {
          continue;
        }
        if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
        }
        return;
      }
      tune(fd);
      auto link = std::make_shared<TcpLink>(fd);
      auto handler = host_->on_accept(link);
      if (!handler) {
        link->close();
        continue;
      }
      spawn_reader(link, std::move(handler));
    }
  }

  bool spawn_reader(const std::shared_ptr<TcpLink>& link, std::shared_ptr<LinkHandler> handler) {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopping_) {
      link->close();
      return false;
    }
    links_.erase(std::remove_if(links_.begin(),
                                links_.end(),
                                [](const std::weak_ptr<TcpLink>& w) { return w.expired(); }),
                 links_.end());
    links_.push_back(link);
    threads_.emplace_back([link, h = std::move(handler)]() { reader_loop(link, h); });
    return true;
  }

  backend::StreamHost* host_ = nullptr;
  unsigned long default_base_ = 40000;
  unsigned long stride_ = 32;
  unsigned long connect_timeout_ms_ = 1000;
  int listen_fd_ = -1;
  std::thread acceptor_;

  std::mutex mu_;
  bool stopping_ = false;
  std::vector<std::weak_ptr<TcpLink>> links_;
  std::vector<std::thread> threads_;
};

}  // namespace

std::unique_ptr<backend::StreamBackend> make_tcp_backend() {
  return std::make_unique<TcpBackend>();
}

}  // namespace pushseg::backends
