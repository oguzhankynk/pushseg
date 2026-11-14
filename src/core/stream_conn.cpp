// Writer side of the stream protocol.
//
// Each attempt to reach the owner gets a fresh link and a WriterLink handler
// tagged with a generation number, so late events of an abandoned link can
// never disturb the current one.

#include <algorithm>
#include <thread>

#include "core/internal.hpp"
#include "core/wire.hpp"

namespace pushseg::detail {
namespace {

constexpr auto kRetryDelay = std::chrono::milliseconds(20);

class StreamConn;

// Per-link handler: decodes control frames coming back from the owner.
class WriterLink final : public backend::LinkHandler, private FrameDecoder::Sink {
public:
  WriterLink(std::weak_ptr<StreamConn> conn, std::uint64_t gen)
      : conn_(std::move(conn)), gen_(gen) {}
  void on_recv(const void* data, std::size_t len) override;
  void on_down(Status reason) override;

private:
  bool on_header(const FrameHeader& h) override;
  bool on_payload(const FrameHeader& /*h*/,
                  std::uint64_t /*pos*/,
                  const std::uint8_t* /*p*/,
                  std::size_t /*n*/) override {
    return false;  // the owner never sends payload
  }
  bool on_end(const FrameHeader& /*h*/) override {
    return true;
  }

  std::weak_ptr<StreamConn> conn_;
  std::uint64_t gen_;
  FrameDecoder dec_;
  bool failed_ = false;
};

class StreamConn final : public ConnImpl, public std::enable_shared_from_this<StreamConn> {
public:
  StreamConn(std::shared_ptr<ContextImpl> ctx,
             backend::StreamBackend& be,
             backend::Endpoint remote,
             SegKey key)
      : ctx_(std::move(ctx)), be_(be), remote_(std::move(remote)), key_(key) {}

  ~StreamConn() override {
    close();
  }

  Status open(Clock::time_point deadline) {
    std::unique_lock<std::mutex> lk(mu_);
    while (true) {
      if (Clock::now() >= deadline) {
        drop_link_locked();
        return Status::kTimeout;
      }
      if (!link_) {
        const std::uint64_t gen = ++gen_;
        auto handler = std::make_shared<WriterLink>(weak_from_this(), gen);
        std::shared_ptr<backend::Link> link;
        lk.unlock();
        const Status st = be_.connect(remote_, handler, link);
        lk.lock();
        if (st != Status::kOk || !link) {
          retry_sleep(lk, deadline);
          continue;
        }
        link_ = std::move(link);
        link_gen_ = gen;
      }
      reply_ = Reply::kNone;
      FrameHeader h;
      h.op = Op::kConnect;
      h.aux = key_.raw();
      h.imm = encode_peer(ctx_->self());
      auto link = link_;
      lk.unlock();
      const Status st = send(*link, h, nullptr, 0);
      lk.lock();
      if (st != Status::kOk) {
        drop_link_locked();
        retry_sleep(lk, deadline);
        continue;
      }
      cv_.wait_until(lk, deadline, [&] { return reply_ != Reply::kNone || link_ != link; });
      if (reply_ == Reply::kOk) {
        state_ = State::kOpen;
        return Status::kOk;
      }
      if (reply_ == Reply::kErr && reply_status_ != Status::kNotFound) {
        drop_link_locked();
        return reply_status_;
      }
      retry_sleep(lk, deadline);  // not published yet, link lost, or deadline
    }
  }

  std::size_t size() const noexcept override {
    return size_;
  }

  Status write(std::uint64_t off, const void* src, std::size_t len) override {
    return send_data(Op::kWrite, off, src, len, 0);
  }

  Status store(std::uint64_t off, std::uint64_t bits, std::size_t width) override {
    const Status chk = check_store(off, width, size_);
    if (chk != Status::kOk) {
      return chk;
    }
    FrameHeader h;
    h.op = Op::kStore;
    h.off = off;
    h.aux = static_cast<std::uint32_t>(width);
    h.imm = bits;
    return send_frame(h, nullptr, 0);
  }

  Status write_notify(std::uint64_t off,
                      const void* src,
                      std::size_t len,
                      std::uint64_t imm) override {
    if (len == 0) {
      return notify(imm);
    }
    return send_data(Op::kWriteNotify, off, src, len, imm);
  }

  Status notify(std::uint64_t imm) override {
    FrameHeader h;
    h.op = Op::kNotify;
    h.imm = imm;
    return send_frame(h, nullptr, 0);
  }

  Status flush(std::chrono::milliseconds timeout) override {
    std::uint64_t id = 0;
    {
      std::lock_guard<std::mutex> lk(mu_);
      id = ++fence_sent_;
    }
    FrameHeader h;
    h.op = Op::kFence;
    h.imm = id;
    const Status st = send_frame(h, nullptr, 0);
    if (st != Status::kOk) {
      return st;
    }
    std::unique_lock<std::mutex> lk(mu_);
    const bool done =
        cv_.wait_for(lk, timeout, [&] { return fence_acked_ >= id || state_ != State::kOpen; });
    if (fence_acked_ >= id) {
      return Status::kOk;
    }
    if (!done) {
      return Status::kTimeout;
    }
    return state_ == State::kClosed ? Status::kClosed : Status::kDisconnected;
  }

  void disconnect() override {
    close();
  }

  void close() {
    std::shared_ptr<backend::Link> link;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (state_ == State::kClosed) {
        return;
      }
      const bool was_open = state_ == State::kOpen;
      state_ = State::kClosed;
      link = std::move(link_);
      link_.reset();
      cv_.notify_all();
      if (!was_open) {
        link.reset();
      }
    }
    if (link) {
      FrameHeader h;
      h.op = Op::kDisconnect;
      std::lock_guard<std::mutex> sl(send_mu_);
      iovec iov{&h, sizeof(h)};
      (void)link->sendv(&iov, 1);
      link->close();
    }
  }

  // Called by WriterLink.
  bool on_ctrl(std::uint64_t gen, const FrameHeader& h) {
    std::lock_guard<std::mutex> lk(mu_);
    if (gen != link_gen_) {
      return true;  // stale link
    }
    switch (h.op) {
      case Op::kConnectOk:
        size_ = h.imm;
        reply_ = Reply::kOk;
        break;
      case Op::kConnectErr:
        reply_ = Reply::kErr;
        reply_status_ = static_cast<Status>(h.aux);
        break;
      case Op::kFenceAck:
        fence_acked_ = std::max(fence_acked_, h.imm);
        break;
      default:
        return false;
    }
    cv_.notify_all();
    return true;
  }

  void on_link_down(std::uint64_t gen) {
    std::lock_guard<std::mutex> lk(mu_);
    if (gen != link_gen_) {
      return;
    }
    if (link_) {
      link_->close();  // idempotent; needed when the link is dropped for a protocol error
    }
    link_.reset();
    link_gen_ = 0;
    if (state_ == State::kOpen) {
      state_ = State::kBroken;
    }
    cv_.notify_all();
  }

private:
  enum class State { kConnecting, kOpen, kBroken, kClosed };
  enum class Reply { kNone, kOk, kErr };

  void drop_link_locked() {
    if (link_) {
      link_->close();
      link_.reset();
    }
    link_gen_ = 0;
  }

  void retry_sleep(std::unique_lock<std::mutex>& lk, Clock::time_point deadline) {
    const auto until = std::min(Clock::now() + kRetryDelay, deadline);
    lk.unlock();
    std::this_thread::sleep_until(until);
    lk.lock();
  }

  Status send(backend::Link& link, const FrameHeader& h, const void* payload, std::size_t len) {
    iovec iov[2];
    iov[0].iov_base = const_cast<FrameHeader*>(&h);
    iov[0].iov_len = sizeof(h);
    iov[1].iov_base = const_cast<void*>(payload);
    iov[1].iov_len = len;
    std::lock_guard<std::mutex> sl(send_mu_);
    return link.sendv(iov, len > 0 ? 2 : 1);
  }

  Status send_frame(const FrameHeader& h, const void* payload, std::size_t len) {
    std::shared_ptr<backend::Link> link;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (state_ != State::kOpen) {
        return state_ == State::kClosed ? Status::kClosed : Status::kDisconnected;
      }
      link = link_;
    }
    const Status st = send(*link, h, payload, len);
    return st == Status::kOk ? st : Status::kDisconnected;
  }

  // Splits large writes; only the last frame carries the notify.
  Status send_data(
      Op last_op, std::uint64_t off, const void* src, std::size_t len, std::uint64_t imm) {
    const Status chk = check_range(off, len, size_);
    if (chk != Status::kOk) {
      return chk;
    }
    const auto* p = static_cast<const std::uint8_t*>(src);
    std::size_t done = 0;
    do {
      const std::size_t chunk = std::min(len - done, kMaxFramePayload);
      FrameHeader h;
      h.op = (done + chunk == len) ? last_op : Op::kWrite;
      h.off = off + done;
      h.len = static_cast<std::uint32_t>(chunk);
      h.imm = imm;
      const Status st = send_frame(h, p + done, chunk);
      if (st != Status::kOk) {
        return st;
      }
      done += chunk;
    } while (done < len);
    return Status::kOk;
  }

  std::shared_ptr<ContextImpl> ctx_;
  backend::StreamBackend& be_;
  backend::Endpoint remote_;
  SegKey key_;

  std::mutex send_mu_;  // keeps frames of concurrent callers from interleaving

  mutable std::mutex mu_;
  std::condition_variable cv_;
  State state_ = State::kConnecting;
  std::shared_ptr<backend::Link> link_;
  std::uint64_t gen_ = 0;
  std::uint64_t link_gen_ = 0;
  Reply reply_ = Reply::kNone;
  Status reply_status_ = Status::kOk;
  std::size_t size_ = 0;
  std::uint64_t fence_sent_ = 0;
  std::uint64_t fence_acked_ = 0;
};

void WriterLink::on_recv(const void* data, std::size_t len) {
  if (failed_) {
    return;
  }
  if (!dec_.feed(static_cast<const std::uint8_t*>(data), len, *this)) {
    failed_ = true;
    if (auto c = conn_.lock()) {
      c->on_link_down(gen_);
    }
  }
}

void WriterLink::on_down(Status /*reason*/) {
  if (auto c = conn_.lock()) {
    c->on_link_down(gen_);
  }
}

bool WriterLink::on_header(const FrameHeader& h) {
  auto c = conn_.lock();
  return c ? c->on_ctrl(gen_, h) : false;
}

}  // namespace

std::shared_ptr<ConnImpl> open_stream_conn(std::shared_ptr<ContextImpl> ctx,
                                           backend::StreamBackend& be,
                                           backend::Endpoint remote,
                                           SegKey key,
                                           Clock::time_point deadline) {
  const NodeId node = remote.node;
  auto conn = std::make_shared<StreamConn>(std::move(ctx), be, std::move(remote), key);
  const Status st = conn->open(deadline);
  if (st != Status::kOk) {
    throw Error(st,
                "connect to node " + std::to_string(node) + " segment " + std::to_string(key.app) +
                    "/" + std::to_string(key.inst) + "/" + std::to_string(key.seg) + " failed");
  }
  return conn;
}

}  // namespace pushseg::detail
