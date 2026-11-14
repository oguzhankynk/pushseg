// Writer side over a MappedBackend: the core turns the memory contract into
// CPU stores, barriers, DMA waits and interrupts, so backends stay simple.
//
//   write        memcpy into the mapping (or DMA above a threshold)
//   ordering pt  wait for queued DMA -> compiler+CPU release fence -> backend
//                store_barrier (sfence for write-combining mappings)
//   store        ordering pt -> single aligned store -> ordering pt
//   notify       ordering pt -> backend interrupt

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "core/internal.hpp"

namespace pushseg::detail {
namespace {

constexpr auto kRetryDelay = std::chrono::milliseconds(20);
constexpr std::size_t kDefaultDmaThreshold = 64 * 1024;

class MappedConn final : public ConnImpl {
public:
  MappedConn(std::shared_ptr<ContextImpl> ctx,
             backend::MappedBackend& be,
             std::size_t dma_threshold)
      : ctx_(std::move(ctx)), be_(be), dma_threshold_(dma_threshold) {}

  ~MappedConn() override {
    close();
  }

  Status open(const backend::Endpoint& remote, SegKey key, Clock::time_point deadline) {
    while (true) {
      const Status st = be_.import_segment(remote, key.raw(), seg_);
      if (st == Status::kOk) {
        open_ = true;
        return st;
      }
      if (st != Status::kNotFound && st != Status::kDisconnected && st != Status::kTimeout) {
        return st;
      }
      if (Clock::now() >= deadline) {
        return Status::kTimeout;
      }
      std::this_thread::sleep_until(std::min(Clock::now() + kRetryDelay, deadline));
    }
  }

  std::size_t size() const noexcept override {
    return seg_.size;
  }

  Status write(std::uint64_t off, const void* src, std::size_t len) override {
    std::lock_guard<std::mutex> lk(mu_);
    if (!open_) {
      return Status::kClosed;
    }
    const Status chk = check_range(off, len, seg_.size);
    if (chk != Status::kOk || len == 0) {
      return chk;
    }
    if (be_.has_dma() && len >= dma_threshold_) {
      const Status st = be_.dma_write(seg_, off, src, len);
      if (st == Status::kOk) {
        dma_pending_ = true;
        // The caller may reuse `src` when we return, so wait here. A future
        // async API can keep the buffer and defer this to the ordering point.
        return order_locked();
      }
      if (st != Status::kUnsupported) {
        return st;
      }
    }
    std::memcpy(const_cast<std::uint8_t*>(seg_.base) + off, src, len);
    return Status::kOk;
  }

  Status store(std::uint64_t off, std::uint64_t bits, std::size_t width) override {
    std::lock_guard<std::mutex> lk(mu_);
    if (!open_) {
      return Status::kClosed;
    }
    const Status chk = check_store(off, width, seg_.size);
    if (chk != Status::kOk) {
      return chk;
    }
    Status st = order_locked();
    if (st != Status::kOk) {
      return st;
    }
    atomic_store_local(const_cast<std::uint8_t*>(seg_.base) + off, bits, width);
    std::atomic_thread_fence(std::memory_order_release);
    be_.store_barrier(seg_);
    return Status::kOk;
  }

  Status write_notify(std::uint64_t off,
                      const void* src,
                      std::size_t len,
                      std::uint64_t imm) override {
    const Status st = write(off, src, len);
    return st == Status::kOk ? notify(imm) : st;
  }

  Status notify(std::uint64_t imm) override {
    std::lock_guard<std::mutex> lk(mu_);
    if (!open_) {
      return Status::kClosed;
    }
    const Status st = order_locked();
    return st == Status::kOk ? be_.interrupt(seg_, imm) : st;
  }

  Status flush(std::chrono::milliseconds /*timeout*/) override {
    std::lock_guard<std::mutex> lk(mu_);
    if (!open_) {
      return Status::kClosed;
    }
    const Status st = order_locked();
    return st == Status::kOk ? be_.flush(seg_) : st;
  }

  void disconnect() override {
    close();
  }

private:
  void close() {
    std::lock_guard<std::mutex> lk(mu_);
    if (!open_) {
      return;
    }
    (void)order_locked();
    open_ = false;
    be_.release_segment(seg_);
  }

  // Everything issued so far becomes visible before anything issued later.
  Status order_locked() {
    if (dma_pending_) {
      dma_pending_ = false;
      const Status st = be_.dma_wait(seg_);
      if (st != Status::kOk) {
        return st;
      }
    }
    std::atomic_thread_fence(std::memory_order_release);
    be_.store_barrier(seg_);
    return Status::kOk;
  }

  std::shared_ptr<ContextImpl> ctx_;  // keeps the backend alive
  backend::MappedBackend& be_;
  std::size_t dma_threshold_;
  std::mutex mu_;
  backend::RemoteSegment seg_;
  bool open_ = false;
  bool dma_pending_ = false;
};

std::size_t dma_threshold_from(const Config& cfg, const std::string& name) {
  const auto b = cfg.backend_options.find(name);
  if (b != cfg.backend_options.end()) {
    const auto o = b->second.find("dma_threshold");
    if (o != b->second.end()) {
      return std::strtoull(o->second.c_str(), nullptr, 10);
    }
  }
  return kDefaultDmaThreshold;
}

}  // namespace

std::shared_ptr<ConnImpl> open_mapped_conn(std::shared_ptr<ContextImpl> ctx,
                                           backend::MappedBackend& be,
                                           backend::Endpoint remote,
                                           SegKey key,
                                           Clock::time_point deadline) {
  const std::string& name = ctx->config().backend_for(ctx->self().node, remote.node);
  const std::size_t threshold = dma_threshold_from(ctx->config(), name);
  auto conn = std::make_shared<MappedConn>(std::move(ctx), be, threshold);
  const Status st = conn->open(remote, key, deadline);
  if (st != Status::kOk) {
    throw Error(st,
                "connect to node " + std::to_string(remote.node) + " segment " +
                    std::to_string(key.app) + "/" + std::to_string(key.inst) + "/" +
                    std::to_string(key.seg) + " failed");
  }
  return conn;
}

}  // namespace pushseg::detail
