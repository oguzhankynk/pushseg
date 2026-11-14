#ifndef PUSHSEG_PUSHSEG_HPP
#define PUSHSEG_PUSHSEG_HPP

/// @file
/// @brief Public API: segments (owner side) and connections (writer side).
///
/// Memory model (identical on every backend):
///
/// 1. Push only. A process reads only its own segments (plain pointer reads).
///    Remote segments are only written, through a Connection.
/// 2. write() is bulk data: no atomicity, no ordering against other write()s.
/// 3. store<T>() (T = 1/2/4/8-byte integer, offset aligned to sizeof(T)) is
///    single-copy atomic: a reader sees the old or the new value, never a mix.
/// 4. store(), notify() and write_notify() are ordering points on their
///    connection: everything sent before becomes visible no later than the
///    point itself, and nothing sent after becomes visible before it.
/// 5. Ordering holds per connection only. Different writers are unordered.
/// 6. Readers must read flags with pushseg::load<T>() (acquire). Data guarded by a
///    flag may then be read with plain loads.
///
/// Typical pattern: write(data) -> store(flag, seq)  or  write_notify(data, imm).

#include "pushseg/config.hpp"
#include "pushseg/status.hpp"
#include "pushseg/types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>

namespace pushseg {

namespace detail {
class ContextImpl;
class SegImpl;
class ConnImpl;
}  // namespace detail

/// @brief Acquire-load of a flag/counter in a local segment.
/// @tparam T 1/2/4/8-byte integer.
/// @param p Address, aligned to sizeof(T).
/// @return The value.
template <typename T>
T load(const void* p) noexcept {
  static_assert(std::is_integral_v<T> &&
                    (sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8),
                "pushseg::load<T>: T must be a 1/2/4/8-byte integer");
  return __atomic_load_n(static_cast<const T*>(p), __ATOMIC_ACQUIRE);
}

/// @brief A local segment: memory that remote writers push into.
///
/// Move-only. Destroying the handle destroys the segment (writers are disconnected).
class LocalSegment {
public:
  LocalSegment() = default;
  ~LocalSegment();
  /// @brief Move-constructs; the source becomes empty.
  /// @param other Source.
  LocalSegment(LocalSegment&& other) noexcept = default;
  /// @brief Move-assigns; destroys the currently held segment first.
  /// @param other Source.
  /// @return *this.
  LocalSegment& operator=(LocalSegment&& other) noexcept;
  LocalSegment(const LocalSegment&) = delete;
  LocalSegment& operator=(const LocalSegment&) = delete;

  /// @brief True if this handle holds a segment.
  /// @return Validity.
  bool valid() const noexcept {
    return impl_ != nullptr;
  }
  /// @brief Local address of offset 0. Page aligned, zero-initialized at creation.
  /// @return Base pointer.
  void* data() const noexcept;
  /// @brief Typed pointer at an offset.
  /// @tparam T Element type.
  /// @param off Byte offset.
  /// @return Pointer.
  template <typename T>
  T* at(std::size_t off = 0) const noexcept {
    return reinterpret_cast<T*>(static_cast<unsigned char*>(data()) + off);
  }
  /// @brief Size in bytes.
  /// @return Size.
  std::size_t size() const noexcept;
  /// @brief Key of the segment (global address together with the node id).
  /// @return Key.
  SegKey key() const noexcept;

  /// @brief Allows writers to connect.
  void publish();
  /// @brief Rejects new writers; existing connections stay.
  void unpublish();
  /// @brief Disconnects all writers and releases the segment.
  void destroy();

  /// @brief Waits for the next event.
  /// @param ev Out: the event.
  /// @param timeout Maximum wait.
  /// @return kOk, kTimeout, or kClosed.
  Status wait_event(Event& ev, std::chrono::milliseconds timeout);
  /// @brief Takes the next event without waiting.
  /// @param ev Out: the event.
  /// @return True if an event was taken.
  bool poll_event(Event& ev);
  /// @brief Delivers future events to `fn` instead of the queue. Runs on a
  /// transport thread: keep it short and never destroy this segment's Context
  /// from it. Pass an empty function to return to queueing.
  /// @param fn Handler.
  void set_event_handler(std::function<void(const Event&)> fn);

private:
  friend class Context;
  explicit LocalSegment(std::shared_ptr<detail::SegImpl> impl);
  std::shared_ptr<detail::SegImpl> impl_;
};

/// @brief One-way write channel into a remote segment.
///
/// Thread-safe; ordering guarantees apply to the sequence of calls as they
/// reach the connection. Move-only; destroying it disconnects.
class Connection {
public:
  Connection() = default;
  ~Connection();
  /// @brief Move-constructs; the source becomes empty.
  /// @param other Source.
  Connection(Connection&& other) noexcept = default;
  /// @brief Move-assigns; disconnects the currently held connection first.
  /// @param other Source.
  /// @return *this.
  Connection& operator=(Connection&& other) noexcept;
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  /// @brief True if this handle holds a connection.
  /// @return Validity.
  bool valid() const noexcept {
    return impl_ != nullptr;
  }
  /// @brief Size of the remote segment.
  /// @return Bytes.
  std::size_t size() const noexcept;

  /// @brief Bulk write. No atomicity, ordered only by the next ordering point.
  /// @param off Offset in the remote segment.
  /// @param src Source bytes (may be reused when the call returns).
  /// @param len Byte count.
  /// @return kOk, kOutOfRange, kDisconnected, kClosed.
  Status write(std::uint64_t off, const void* src, std::size_t len);

  /// @brief Single-copy atomic store, ordering point.
  /// @tparam T 1/2/4/8-byte integer.
  /// @param off Offset, aligned to sizeof(T).
  /// @param value Value.
  /// @return kOk, kMisaligned, kOutOfRange, kDisconnected, kClosed.
  template <typename T>
  Status store(std::uint64_t off, T value) {
    static_assert(std::is_integral_v<T> &&
                      (sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8),
                  "Connection::store<T>: T must be a 1/2/4/8-byte integer");
    std::uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(T));
    return store_raw(off, bits, sizeof(T));
  }

  /// @brief write() followed by notify(imm), as one ordering point.
  /// @param off Offset.
  /// @param src Source bytes.
  /// @param len Byte count (0 = just notify).
  /// @param imm Value delivered in the owner's kNotify event.
  /// @return Same as write().
  Status write_notify(std::uint64_t off, const void* src, std::size_t len, std::uint64_t imm);

  /// @brief Raises a kNotify event at the owner. Ordering point.
  /// @param imm Value delivered with the event.
  /// @return kOk, kDisconnected, kClosed.
  Status notify(std::uint64_t imm);

  /// @brief Waits until everything sent so far has landed in remote memory.
  /// @param timeout Maximum wait.
  /// @return kOk, kTimeout, kDisconnected, kClosed.
  Status flush(std::chrono::milliseconds timeout = std::chrono::seconds(5));

  /// @brief Closes the connection (the owner gets kDisconnect). Idempotent.
  void disconnect();

private:
  friend class Context;
  explicit Connection(std::shared_ptr<detail::ConnImpl> impl);
  Status store_raw(std::uint64_t off, std::uint64_t bits, std::size_t width);
  std::shared_ptr<detail::ConnImpl> impl_;
};

/// @brief One process's view of the system. Starts the backends it needs.
///
/// Move-only. Destroying it closes every segment and connection it created
/// (their handles then return kClosed).
class Context {
public:
  /// @brief Creates the context for process (node, app, inst).
  ///
  /// Throws pushseg::Error on bad config or when a backend fails to start
  /// (e.g. the same (node, app, inst) is already running).
  /// @param cfg System config.
  /// @param node This node.
  /// @param app Application name from [apps].
  /// @param inst Instance number of this application on this node.
  Context(const Config& cfg, NodeId node, const std::string& app, InstId inst = 0);
  ~Context();
  /// @brief Move-constructs; the source becomes empty.
  /// @param other Source.
  Context(Context&& other) noexcept = default;
  /// @brief Move-assigns; shuts down the currently held context first.
  /// @param other Source.
  /// @return *this.
  Context& operator=(Context&& other) noexcept;
  Context(const Context&) = delete;
  Context& operator=(const Context&) = delete;

  /// @brief This process's identity.
  /// @return Peer id.
  PeerId self() const noexcept;

  /// @brief Creates a local segment with key (self.app, self.inst, seg).
  /// It is not connectable until publish(). Throws pushseg::Error (kExists, ...).
  /// @param seg Segment number.
  /// @param size Bytes (> 0).
  /// @return The segment.
  LocalSegment create_segment(SegNo seg, std::size_t size);

  /// @brief Connects to a remote segment, retrying until it is published.
  /// Throws pushseg::Error (kTimeout, kInvalidArgument, ...).
  /// @param node Owner node.
  /// @param key Segment key.
  /// @param timeout Maximum time to wait for the segment to appear.
  /// @return The connection.
  Connection connect(NodeId node,
                     SegKey key,
                     std::chrono::milliseconds timeout = std::chrono::seconds(10));

  /// @brief Convenience overload: key from application name.
  /// @param node Owner node.
  /// @param app Owner application name.
  /// @param inst Owner instance.
  /// @param seg Segment number.
  /// @param timeout Maximum wait.
  /// @return The connection.
  Connection connect(NodeId node,
                     const std::string& app,
                     InstId inst,
                     SegNo seg,
                     std::chrono::milliseconds timeout = std::chrono::seconds(10));

private:
  std::shared_ptr<detail::ContextImpl> impl_;
};

}  // namespace pushseg

#endif  // PUSHSEG_PUSHSEG_HPP
