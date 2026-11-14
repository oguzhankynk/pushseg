// Built-in stream backend over POSIX message queues (mq_overview(7)).
// Same host only: every node linked through "mq" must run on one machine.
//
// Naming (options from [backend.mq]):
//   listener queue   <name_prefix>.<node>.<app>.<inst>          one per process
//   lock file        <lock_dir>/<prefix>.<node>.<app>.<inst>.lock
//   link queues      <name_prefix>.l<pid>.<n>.c2s / .s2c         two per link
//
// The lock file (flock) makes a (node, app, inst) identity unique on the
// host, lets a restarted process remove a stale listener queue, and holds
// the owner's pid so that clients can detect a dead peer: message queues do
// not report that the other side went away.
//
// A link is two queues, one per direction. The connecting side creates both
// and sends their names to the listener, which opens and unlinks them, so
// they disappear once both ends close. A zero-length message means EOF.
// The byte stream is cut into messages of at most the queue's msgsize.
//
// Limits: msgsize <= /proc/sys/fs/mqueue/msgsize_max, maxmsg <=
// /proc/sys/fs/mqueue/msg_max, and the per-user RLIMIT_MSGQUEUE (ulimit -q,
// often 800 KiB) caps the total: each link costs about 2 * maxmsg * msgsize.

#include "pushseg/backend.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <mutex>
#include <string>
#include <sys/eventfd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <mqueue.h>
#include <poll.h>
#include <unistd.h>

#include "backends/posix_common.hpp"

namespace pushseg::backends {
namespace {

using backend::Endpoint;
using backend::Link;
using backend::LinkHandler;
using backend::Options;
using posix::alive;
using posix::deadline_in;
using posix::identity;
using posix::mq_acquire;
using posix::mq_release;
using posix::opt;
using posix::opt_long;

constexpr int kPollMs = 200;                     // peer liveness check period
constexpr std::uint32_t kReqMagic = 0x50534D51;  // "QMSP"
constexpr std::size_t kNameMax = 96;
constexpr long kListenMaxMsg = 8;
const mqd_t kBadMq = -1;  // mq_open() error value; mqd_t is an fd on Linux

struct ConnReq {
  std::uint32_t magic = kReqMagic;
  std::int32_t pid = 0;
  char c2s[kNameMax] = {};
  char s2c[kNameMax] = {};
};

std::size_t queue_msgsize(mqd_t q) {
  mq_attr a{};
  return ::mq_getattr(q, &a) == 0 && a.mq_msgsize > 0 ? static_cast<std::size_t>(a.mq_msgsize) : 0;
}

class MqLink final : public Link {
public:
  MqLink(mqd_t send_q, mqd_t recv_q, pid_t peer, std::vector<std::string> names)
      : send_q_(send_q),
        recv_q_(recv_q),
        peer_(peer),
        names_(std::move(names)),
        send_size_(queue_msgsize(send_q)),
        recv_size_(queue_msgsize(recv_q)),
        wake_fd_(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)),
        stage_(send_size_) {}

  ~MqLink() override {
    ::mq_close(send_q_);
    ::mq_close(recv_q_);
    for (const auto& n : names_) {
      ::mq_unlink(n.c_str());  // usually already unlinked by the listener
    }
    if (wake_fd_ >= 0) {
      ::close(wake_fd_);
    }
  }
  MqLink(const MqLink&) = delete;
  MqLink& operator=(const MqLink&) = delete;

  bool ok() const noexcept {
    return wake_fd_ >= 0 && send_size_ > 0 && recv_size_ > 0;
  }

  Status sendv(const struct iovec* iov, int iovcnt) override {
    std::lock_guard<std::mutex> lk(send_mu_);
    std::size_t fill = 0;
    for (int i = 0; i < iovcnt; ++i) {
      const auto* p = static_cast<const char*>(iov[i].iov_base);
      std::size_t left = iov[i].iov_len;
      while (left > 0) {
        if (fill == 0 && left >= send_size_) {  // whole message straight from the caller
          const Status st = send_msg(p, send_size_);
          if (st != Status::kOk) {
            return st;
          }
          p += send_size_;
          left -= send_size_;
          continue;
        }
        const std::size_t take = std::min(left, send_size_ - fill);
        std::memcpy(stage_.data() + fill, p, take);
        fill += take;
        p += take;
        left -= take;
        if (fill == send_size_) {
          const Status st = send_msg(stage_.data(), fill);
          if (st != Status::kOk) {
            return st;
          }
          fill = 0;
        }
      }
    }
    return fill > 0 ? send_msg(stage_.data(), fill) : Status::kOk;
  }

  void close() override {
    if (!closed_.exchange(true)) {
      const std::uint64_t one = 1;
      (void)!::write(wake_fd_, &one, sizeof(one));
    }
  }

  bool closed() const noexcept {
    return closed_.load(std::memory_order_acquire);
  }

  // Reader side, used only by the link's reader thread.
  mqd_t recv_q() const noexcept {
    return recv_q_;
  }
  int wake_fd() const noexcept {
    return wake_fd_;
  }
  pid_t peer() const noexcept {
    return peer_;
  }
  std::size_t recv_size() const noexcept {
    return recv_size_;
  }
  // Best effort: tell the peer we are gone.
  void send_eof() {
    const timespec ts = deadline_in(100);
    mq_release();
    (void)::mq_timedsend(send_q_, "", 0, 0, &ts);
  }

private:
  Status send_msg(const char* p, std::size_t n) {
    while (true) {
      if (closed()) {
        return Status::kClosed;
      }
      const timespec ts = deadline_in(kPollMs);
      mq_release();
      if (::mq_timedsend(send_q_, p, n, 0, &ts) == 0) {
        return Status::kOk;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == ETIMEDOUT) {  // queue full: keep waiting while the peer lives
        if (!alive(peer_)) {
          return Status::kDisconnected;
        }
        continue;
      }
      return Status::kDisconnected;
    }
  }

  mqd_t send_q_;
  mqd_t recv_q_;
  pid_t peer_;
  std::vector<std::string> names_;
  std::size_t send_size_;
  std::size_t recv_size_;
  int wake_fd_;
  std::atomic<bool> closed_{false};
  std::mutex send_mu_;
  std::vector<char> stage_;  // guarded by send_mu_
};

void reader_loop(const std::shared_ptr<MqLink>& link, const std::shared_ptr<LinkHandler>& h) {
  std::vector<char> buf(link->recv_size());
  Status why = Status::kDisconnected;
  while (true) {
    pollfd fds[2] = {{link->recv_q(), POLLIN, 0}, {link->wake_fd(), POLLIN, 0}};
    const int r = ::poll(fds, 2, kPollMs);
    if (r < 0 && errno != EINTR) {
      why = Status::kIoError;
      break;
    }
    if (link->closed()) {
      link->send_eof();
      why = Status::kClosed;
      break;
    }
    if (r <= 0) {
      if (!alive(link->peer())) {
        why = Status::kDisconnected;
        break;
      }
      continue;
    }
    if ((fds[0].revents & POLLIN) == 0) {
      continue;
    }
    const ssize_t n = ::mq_receive(link->recv_q(), buf.data(), buf.size(), nullptr);
    mq_acquire();
    if (n < 0) {
      if (errno == EAGAIN || errno == EINTR) {
        continue;
      }
      why = Status::kIoError;
      break;
    }
    if (n == 0) {  // EOF from the peer
      why = Status::kDisconnected;
      break;
    }
    h->on_recv(buf.data(), static_cast<std::size_t>(n));
  }
  link->close();
  h->on_down(why);
}

class MqBackend final : public backend::StreamBackend {
public:
  ~MqBackend() override {
    shutdown();
  }

  Status start(const Endpoint& self, const Options& opts, backend::StreamHost& host) override {
    host_ = &host;
    prefix_ = opt(opts, "name_prefix", "/pushseg");
    lock_dir_ = opt(opts, "lock_dir", "/dev/shm");
    msgsize_ = opt_long(opts, "msgsize", 8192);
    maxmsg_ = opt_long(opts, "maxmsg", 8);
    if (prefix_.size() < 2 || prefix_[0] != '/' || prefix_.find('/', 1) != std::string::npos ||
        msgsize_ <= 0 || maxmsg_ <= 0) {
      return Status::kInvalidArgument;
    }

    // Identity lock: fails while another live process holds it.
    if (const Status st = lock_.acquire(lock_path(self)); st != Status::kOk) {
      return st;
    }

    // Holding the lock, any existing listener queue is stale.
    listen_name_ = prefix_ + "." + identity(self);
    ::mq_unlink(listen_name_.c_str());
    mq_attr attr{};
    attr.mq_maxmsg = kListenMaxMsg;
    attr.mq_msgsize = static_cast<long>(sizeof(ConnReq));
    listen_q_ = ::mq_open(
        listen_name_.c_str(), O_RDONLY | O_CREAT | O_EXCL | O_NONBLOCK | O_CLOEXEC, 0600, &attr);
    wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (listen_q_ == kBadMq || wake_fd_ < 0) {
      shutdown();
      return Status::kIoError;
    }
    acceptor_ = std::thread([this] { accept_loop(); });
    return Status::kOk;
  }

  Status connect(const Endpoint& remote,
                 std::shared_ptr<LinkHandler> handler,
                 std::shared_ptr<Link>& out) override {
    const pid_t peer = read_owner_pid(remote);
    if (peer <= 0 || !alive(peer)) {
      return Status::kNotFound;  // not started yet, or stale
    }
    const std::string rname = prefix_ + "." + identity(remote);
    const mqd_t rq = ::mq_open(rname.c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC);
    if (rq == kBadMq) {
      return Status::kNotFound;
    }

    const std::string base =
        prefix_ + ".l" + std::to_string(::getpid()) + "." + std::to_string(++link_seq_);
    const std::string c2s = base + ".c2s";
    const std::string s2c = base + ".s2c";
    mq_attr attr{};
    attr.mq_maxmsg = maxmsg_;
    attr.mq_msgsize = msgsize_;
    const mqd_t send_q =
        ::mq_open(c2s.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600, &attr);
    const mqd_t recv_q =
        ::mq_open(s2c.c_str(), O_RDONLY | O_CREAT | O_EXCL | O_NONBLOCK | O_CLOEXEC, 0600, &attr);
    const int open_errno = errno;
    auto link = std::make_shared<MqLink>(send_q, recv_q, peer, std::vector<std::string>{c2s, s2c});
    if (send_q == kBadMq || recv_q == kBadMq || !link->ok()) {
      ::mq_close(rq);
      // EINVAL: msgsize/maxmsg above the system limits.
      return open_errno == EINVAL ? Status::kInvalidArgument : Status::kIoError;
    }

    ConnReq req;
    req.pid = ::getpid();
    std::strncpy(req.c2s, c2s.c_str(), kNameMax - 1);
    std::strncpy(req.s2c, s2c.c_str(), kNameMax - 1);
    mq_release();
    const int rc = ::mq_send(rq, reinterpret_cast<const char*>(&req), sizeof(req), 0);
    ::mq_close(rq);
    if (rc != 0) {
      return Status::kNotFound;  // listener backlog full: the core retries
    }
    if (!spawn_reader(link, std::move(handler))) {
      return Status::kClosed;
    }
    out = link;
    return Status::kOk;
  }

  void stop() override {
    shutdown();
  }

private:
  std::string lock_path(const Endpoint& e) const {
    return lock_dir_ + "/" + prefix_.substr(1) + "." + identity(e) + ".lock";
  }

  pid_t read_owner_pid(const Endpoint& e) const {
    return posix::read_pid(lock_path(e));
  }

  void accept_loop() {
    while (true) {
      pollfd fds[2] = {{listen_q_, POLLIN, 0}, {wake_fd_, POLLIN, 0}};
      if (::poll(fds, 2, -1) < 0 && errno != EINTR) {
        return;
      }
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (stopping_) {
          return;
        }
      }
      ConnReq req;
      const ssize_t n =
          ::mq_receive(listen_q_, reinterpret_cast<char*>(&req), sizeof(req), nullptr);
      mq_acquire();
      if (n != static_cast<ssize_t>(sizeof(req)) || req.magic != kReqMagic) {
        continue;
      }
      req.c2s[kNameMax - 1] = '\0';
      req.s2c[kNameMax - 1] = '\0';
      const mqd_t recv_q = ::mq_open(req.c2s, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
      const mqd_t send_q = ::mq_open(req.s2c, O_WRONLY | O_CLOEXEC);
      ::mq_unlink(req.c2s);  // both ends are open (or the client is gone): drop the names
      ::mq_unlink(req.s2c);
      auto link = std::make_shared<MqLink>(send_q, recv_q, req.pid, std::vector<std::string>{});
      if (send_q == kBadMq || recv_q == kBadMq || !link->ok()) {
        continue;
      }
      auto handler = host_->on_accept(link);
      if (!handler) {
        link->send_eof();
        continue;
      }
      spawn_reader(link, std::move(handler));
    }
  }

  bool spawn_reader(const std::shared_ptr<MqLink>& link, std::shared_ptr<LinkHandler> handler) {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopping_) {
      return false;
    }
    links_.erase(std::remove_if(links_.begin(),
                                links_.end(),
                                [](const std::weak_ptr<MqLink>& w) { return w.expired(); }),
                 links_.end());
    links_.push_back(link);
    threads_.emplace_back([link, h = std::move(handler)]() { reader_loop(link, h); });
    return true;
  }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (stopping_) {
        return;
      }
      stopping_ = true;
    }
    if (wake_fd_ >= 0) {
      const std::uint64_t one = 1;
      (void)!::write(wake_fd_, &one, sizeof(one));
    }
    if (acceptor_.joinable()) {
      acceptor_.join();
    }
    std::vector<std::weak_ptr<MqLink>> links;
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
    if (listen_q_ != kBadMq) {
      ::mq_close(listen_q_);
      ::mq_unlink(listen_name_.c_str());
      listen_q_ = kBadMq;
    }
    if (wake_fd_ >= 0) {
      ::close(wake_fd_);
      wake_fd_ = -1;
    }
    lock_.release();
  }

  backend::StreamHost* host_ = nullptr;
  std::string prefix_;
  std::string lock_dir_;
  std::string listen_name_;
  long msgsize_ = 8192;
  long maxmsg_ = 8;
  posix::IdentityLock lock_;
  int wake_fd_ = -1;
  mqd_t listen_q_ = kBadMq;
  std::atomic<std::uint64_t> link_seq_{0};
  std::thread acceptor_;

  std::mutex mu_;
  bool stopping_ = false;
  std::vector<std::weak_ptr<MqLink>> links_;
  std::vector<std::thread> threads_;
};

}  // namespace

std::unique_ptr<backend::StreamBackend> make_mq_backend() {
  return std::make_unique<MqBackend>();
}

}  // namespace pushseg::backends
