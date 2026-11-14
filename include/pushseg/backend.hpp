#ifndef PUSHSEG_BACKEND_HPP
#define PUSHSEG_BACKEND_HPP

/// @file
/// @brief Backend SPI: how a transport API is plugged into pushseg.
///
/// There are two integration levels. Pick the one that matches your API:
///
/// - **Stream** (StreamBackend): your API moves bytes reliably and in order
///   (TCP, a reliable message channel, a serial link...). You implement
///   connect/listen/send and hand received bytes to the core. The core runs the
///   whole segment protocol on top: framing, writes, atomic stores, notify,
///   fences. You never have to think about atomicity or ordering.
///
/// - **Mapped** (MappedBackend): your API exposes remote memory as a mapped
///   pointer (PCIe NTB/BAR style) plus a way to raise an interrupt. You
///   implement allocate/export/import/barrier/interrupt. The core implements
///   write (memcpy or your DMA), store (barrier + aligned store + barrier) and
///   notify (barrier + interrupt).
///
/// Backends are registered by name before a Context is created and selected
/// per node pair by the config file ([links]).

#include "pushseg/status.hpp"
#include "pushseg/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <sys/uio.h>

namespace pushseg::backend {

/// @brief Options from the config's [backend.NAME] section.
using Options = std::map<std::string, std::string>;

/// @brief A process reachable through a backend.
struct Endpoint {
  NodeId node = 0;      ///< Node number.
  AppId app = 0;        ///< Application id of the process.
  InstId inst = 0;      ///< Instance id of the process.
  std::string address;  ///< Backend-specific address from the config ([node.N] NAME = ...).
};

// ---------------------------------------------------------------------------
// Stream level
// ---------------------------------------------------------------------------

/// @brief Receives events of one link. Implemented by the core.
///
/// Contract for the backend:
/// - Calls on one link are serialized (never concurrent with each other).
/// - Bytes are delivered in exactly the order they were sent, without loss or
///   duplication. Chunk boundaries do not matter.
/// - on_down() is called exactly once per link, and it is the last call.
///   It is also called after a local Link::close().
class LinkHandler {
public:
  virtual ~LinkHandler() = default;
  /// @brief Bytes arrived.
  /// @param data Pointer valid only during the call.
  /// @param len Number of bytes (> 0).
  virtual void on_recv(const void* data, std::size_t len) = 0;
  /// @brief The link is gone. No further calls follow.
  /// @param reason kClosed after a local close, otherwise why it broke.
  virtual void on_down(Status reason) = 0;
};

/// @brief One bidirectional, reliable, ordered byte stream. Implemented by the backend.
class Link {
public:
  virtual ~Link() = default;
  /// @brief Sends all bytes of the vector, in order, blocking until handed to the transport.
  ///
  /// May be called from several threads; the backend must not interleave the
  /// bytes of two concurrent calls.
  /// @param iov Buffers.
  /// @param iovcnt Number of buffers (the core uses at most 2).
  /// @return kOk, or an error once the link is unusable.
  virtual Status sendv(const struct iovec* iov, int iovcnt) = 0;
  /// @brief Closes the link. Idempotent, non-blocking, callable from any thread
  /// including from inside a LinkHandler call. on_down() follows.
  virtual void close() = 0;
};

/// @brief Callbacks a stream backend makes into the core. Implemented by the core.
class StreamHost {
public:
  virtual ~StreamHost() = default;
  /// @brief A remote peer opened a link to us.
  ///
  /// Must be called before any data of that link is delivered.
  /// @param link The new link.
  /// @return Handler for the link, or nullptr to reject (the backend then closes it).
  virtual std::shared_ptr<LinkHandler> on_accept(std::shared_ptr<Link> link) = 0;
};

/// @brief A byte-stream transport.
class StreamBackend {
public:
  virtual ~StreamBackend() = default;
  /// @brief Starts accepting links addressed to `self`.
  /// @param self This process.
  /// @param opts Config options.
  /// @param host Core callbacks; outlives the backend's activity until stop().
  /// @return kOk or an error.
  virtual Status start(const Endpoint& self, const Options& opts, StreamHost& host) = 0;
  /// @brief Opens a link to a remote process. Should fail fast when the peer is
  /// not listening; the core retries until its own deadline.
  /// @param remote Target process.
  /// @param handler Receives the link's events.
  /// @param out The new link on success.
  /// @return kOk or an error.
  virtual Status connect(const Endpoint& remote,
                         std::shared_ptr<LinkHandler> handler,
                         std::shared_ptr<Link>& out) = 0;
  /// @brief Closes all links, stops all threads. No upcalls after it returns.
  /// Never called from inside an upcall.
  virtual void stop() = 0;
};

// ---------------------------------------------------------------------------
// Mapped level
// ---------------------------------------------------------------------------

/// @brief A remote segment mapped into this process.
struct RemoteSegment {
  volatile std::uint8_t* base = nullptr;  ///< Mapped address of offset 0.
  std::size_t size = 0;                   ///< Segment size in bytes.
  void* handle = nullptr;                 ///< Backend-private.
};

/// @brief Callbacks a mapped backend makes into the core. Implemented by the core.
/// May be called from any thread.
class MappedHost {
public:
  virtual ~MappedHost() = default;
  /// @brief An interrupt for one of our segments arrived.
  /// @param seg_key Raw SegKey of the local segment.
  /// @param from Sender, if the backend knows it.
  /// @param imm Value passed to interrupt().
  virtual void on_interrupt(std::uint32_t seg_key, const PeerId& from, std::uint64_t imm) = 0;
  /// @brief Optional: a peer imported one of our segments.
  /// @param seg_key Raw SegKey.
  /// @param peer The importer.
  virtual void on_peer_connect(std::uint32_t seg_key, const PeerId& peer) = 0;
  /// @brief Optional: a peer released one of our segments.
  /// @param seg_key Raw SegKey.
  /// @param peer The importer.
  /// @param lost True if the peer vanished instead of releasing.
  virtual void on_peer_disconnect(std::uint32_t seg_key, const PeerId& peer, bool lost) = 0;
};

/// @brief A memory-mapping transport (PCIe and similar).
///
/// When a node uses a mapped backend, all of that node's segments are
/// allocated through it, so the same memory can also be written by stream
/// links. At most one mapped backend per node.
class MappedBackend {
public:
  virtual ~MappedBackend() = default;
  /// @brief Initializes the backend.
  /// @param self This process.
  /// @param opts Config options.
  /// @param host Core callbacks.
  /// @return kOk or an error.
  virtual Status start(const Endpoint& self, const Options& opts, MappedHost& host) = 0;
  /// @brief Allocates zero-initialized, remotely writable memory for a local segment.
  /// @param key Raw SegKey (unique per node).
  /// @param size Bytes.
  /// @param base Out: local address.
  /// @return kOk, kExists, or an error.
  virtual Status alloc_segment(std::uint32_t key, std::size_t size, void** base) = 0;
  /// @brief Makes the segment importable.
  /// @param key Raw SegKey.
  /// @return kOk or an error.
  virtual Status export_segment(std::uint32_t key) = 0;
  /// @brief Stops new imports of the segment.
  /// @param key Raw SegKey.
  virtual void unexport_segment(std::uint32_t key) = 0;
  /// @brief Frees the segment. Must also work after stop().
  /// @param key Raw SegKey.
  virtual void free_segment(std::uint32_t key) = 0;
  /// @brief Maps a remote segment. Return kNotFound if it is not (yet) exported:
  /// the core retries until its deadline.
  /// @param remote Owner process.
  /// @param key Raw SegKey.
  /// @param out Mapping.
  /// @return kOk or an error.
  virtual Status import_segment(const Endpoint& remote, std::uint32_t key, RemoteSegment& out) = 0;
  /// @brief Unmaps a remote segment.
  /// @param seg Mapping from import_segment.
  virtual void release_segment(RemoteSegment& seg) = 0;
  /// @brief Makes all previous CPU stores to the mapping globally visible before
  /// any later store. Default: x86 sfence (correct for write-combining mappings).
  /// @param seg Mapping.
  virtual void store_barrier(const RemoteSegment& seg);
  /// @brief Waits until all previous writes have landed in remote memory.
  /// Default: store_barrier(). Override if your API offers a stronger flush.
  /// @param seg Mapping.
  /// @return kOk or an error.
  virtual Status flush(const RemoteSegment& seg);
  /// @brief Raises an interrupt on the segment's owner. The core has already
  /// ordered all previous writes before calling this.
  /// @param seg Mapping.
  /// @param imm Value delivered to MappedHost::on_interrupt.
  /// @return kOk or an error.
  virtual Status interrupt(const RemoteSegment& seg, std::uint64_t imm) = 0;
  /// @brief Optional DMA engine. Default: none (the core uses memcpy).
  /// @return True if dma_write/dma_wait are implemented.
  virtual bool has_dma() const {
    return false;
  }
  /// @brief Queues an asynchronous copy into the remote segment.
  /// @param seg Mapping.
  /// @param off Offset in the segment.
  /// @param src Source; must stay valid until dma_wait() returns.
  /// @param len Bytes.
  /// @return kOk or an error.
  virtual Status dma_write(const RemoteSegment& seg,
                           std::uint64_t off,
                           const void* src,
                           std::size_t len);
  /// @brief Waits for all DMA queued on `seg` to land in remote memory.
  /// @param seg Mapping.
  /// @return kOk or an error.
  virtual Status dma_wait(const RemoteSegment& seg);
  /// @brief Stops the backend. No upcalls after it returns.
  virtual void stop() = 0;
};

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// @brief Creates a stream backend instance (one per Context).
using StreamFactory = std::function<std::unique_ptr<StreamBackend>()>;
/// @brief Creates a mapped backend instance (one per Context).
using MappedFactory = std::function<std::unique_ptr<MappedBackend>()>;

/// @brief Registers a stream backend under `name`. Replaces an existing one.
/// Built-in: "tcp", "mq" (POSIX message queues, same host only).
/// @param name Name used in the config file.
/// @param factory Instance factory.
void register_stream_backend(const std::string& name, StreamFactory factory);

/// @brief Registers a mapped backend under `name`. Replaces an existing one.
/// Built-in: "shm" (POSIX shared memory, same host only) and "loopmem"
/// (in-process reference/test backend).
/// @param name Name used in the config file.
/// @param factory Instance factory.
void register_mapped_backend(const std::string& name, MappedFactory factory);

}  // namespace pushseg::backend

#endif  // PUSHSEG_BACKEND_HPP
