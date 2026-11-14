#ifndef PUSHSEG_SRC_CORE_INTERNAL_HPP
#define PUSHSEG_SRC_CORE_INTERNAL_HPP

#include "pushseg/backend.hpp"
#include "pushseg/config.hpp"
#include "pushseg/status.hpp"
#include "pushseg/types.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace pushseg::detail {

using Clock = std::chrono::steady_clock;

class ContextImpl;

// Owner-side state of one local segment.
class SegImpl {
public:
  SegImpl(std::shared_ptr<ContextImpl> ctx,
          SegKey key,
          std::uint8_t* base,
          std::size_t size,
          bool mapped_alloc);
  ~SegImpl();
  SegImpl(const SegImpl&) = delete;
  SegImpl& operator=(const SegImpl&) = delete;

  SegKey key() const noexcept {
    return key_;
  }
  std::uint8_t* base() const noexcept {
    return base_;
  }
  std::size_t size() const noexcept {
    return size_;
  }
  bool published() const noexcept {
    return published_.load(std::memory_order_acquire);
  }
  bool destroyed() const noexcept {
    return destroyed_.load(std::memory_order_acquire);
  }

  void publish();
  void unpublish();
  void destroy();

  void push_event(const Event& ev);
  Status wait_event(Event& ev, std::chrono::milliseconds timeout);
  bool poll_event(Event& ev);
  void set_handler(std::function<void(const Event&)> fn);

  // Stream links attached to this segment, closed on destroy().
  void add_link(const std::shared_ptr<backend::Link>& link);

private:
  std::shared_ptr<ContextImpl> ctx_;
  SegKey key_;
  std::uint8_t* base_;
  std::size_t size_;
  bool mapped_alloc_;
  std::atomic<bool> published_{false};
  std::atomic<bool> destroyed_{false};

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Event> events_;
  std::shared_ptr<std::function<void(const Event&)>> handler_;
  std::vector<std::weak_ptr<backend::Link>> links_;
};

// Writer-side connection, one implementation per backend level.
class ConnImpl {
public:
  virtual ~ConnImpl() = default;
  virtual std::size_t size() const noexcept = 0;
  virtual Status write(std::uint64_t off, const void* src, std::size_t len) = 0;
  virtual Status store(std::uint64_t off, std::uint64_t bits, std::size_t width) = 0;
  virtual Status write_notify(std::uint64_t off,
                              const void* src,
                              std::size_t len,
                              std::uint64_t imm) = 0;
  virtual Status notify(std::uint64_t imm) = 0;
  virtual Status flush(std::chrono::milliseconds timeout) = 0;
  virtual void disconnect() = 0;
};

// Shared argument checks.
inline Status check_range(std::uint64_t off, std::size_t len, std::size_t size) noexcept {
  return (off > size || len > size - off) ? Status::kOutOfRange : Status::kOk;
}
inline Status check_store(std::uint64_t off, std::size_t width, std::size_t size) noexcept {
  if (width != 1 && width != 2 && width != 4 && width != 8) {
    return Status::kInvalidArgument;
  }
  if (off % width != 0) {
    return Status::kMisaligned;
  }
  return check_range(off, width, size);
}

// Release-store of a 1/2/4/8-byte value into local memory.
void atomic_store_local(std::uint8_t* p, std::uint64_t bits, std::size_t width) noexcept;

class ContextImpl final : public std::enable_shared_from_this<ContextImpl>,
                          public backend::StreamHost,
                          public backend::MappedHost {
public:
  ContextImpl(Config cfg, PeerId self);
  ~ContextImpl() override;
  ContextImpl(const ContextImpl&) = delete;
  ContextImpl& operator=(const ContextImpl&) = delete;

  // Must be called once right after construction (needs shared_from_this).
  void start();
  void shutdown();
  bool stopped() const noexcept {
    return stopped_.load(std::memory_order_acquire);
  }

  const PeerId& self() const noexcept {
    return self_;
  }
  const Config& config() const noexcept {
    return cfg_;
  }
  backend::MappedBackend* mapped() const noexcept {
    return mapped_.get();
  }

  std::shared_ptr<SegImpl> create_segment(SegNo seg, std::size_t size);
  std::shared_ptr<ConnImpl> connect(NodeId node, SegKey key, std::chrono::milliseconds timeout);

  // Used by SegImpl.
  void seg_published(SegImpl& seg);
  void seg_unpublished(SegImpl& seg);
  void seg_released(SegKey key, std::uint8_t* base, bool mapped_alloc) noexcept;

  // Used by inbound stream links.
  std::shared_ptr<SegImpl> find_published(std::uint32_t key);

  // StreamHost
  std::shared_ptr<backend::LinkHandler> on_accept(std::shared_ptr<backend::Link> link) override;
  // MappedHost
  void on_interrupt(std::uint32_t seg_key, const PeerId& from, std::uint64_t imm) override;
  void on_peer_connect(std::uint32_t seg_key, const PeerId& peer) override;
  void on_peer_disconnect(std::uint32_t seg_key, const PeerId& peer, bool lost) override;

private:
  backend::Endpoint endpoint(NodeId node, AppId app, InstId inst, const std::string& be) const;
  std::shared_ptr<SegImpl> find_segment(std::uint32_t key);

  Config cfg_;
  PeerId self_;
  std::atomic<bool> stopped_{false};

  std::map<std::string, std::unique_ptr<backend::StreamBackend>> streams_;
  std::string mapped_name_;
  std::unique_ptr<backend::MappedBackend> mapped_;

  std::mutex create_mu_;  // serializes create_segment()
  std::mutex mu_;         // guards the maps below; never held across a backend call
  std::map<std::uint32_t, std::weak_ptr<SegImpl>> segments_;
  std::vector<std::weak_ptr<ConnImpl>> conns_;
};

// Backend registry (registry.cpp).
std::unique_ptr<backend::StreamBackend> make_stream_backend(const std::string& name);
std::unique_ptr<backend::MappedBackend> make_mapped_backend(const std::string& name);

// Connection implementations.
std::shared_ptr<ConnImpl> open_stream_conn(std::shared_ptr<ContextImpl> ctx,
                                           backend::StreamBackend& be,
                                           backend::Endpoint remote,
                                           SegKey key,
                                           Clock::time_point deadline);
std::shared_ptr<ConnImpl> open_mapped_conn(std::shared_ptr<ContextImpl> ctx,
                                           backend::MappedBackend& be,
                                           backend::Endpoint remote,
                                           SegKey key,
                                           Clock::time_point deadline);
std::shared_ptr<backend::LinkHandler> make_inbound_handler(std::shared_ptr<ContextImpl> ctx,
                                                           std::shared_ptr<backend::Link> link);

}  // namespace pushseg::detail

#endif  // PUSHSEG_SRC_CORE_INTERNAL_HPP
