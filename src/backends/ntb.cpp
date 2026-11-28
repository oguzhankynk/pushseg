// Built-in mapped backend "ntb": PCIe NTB (non-transparent bridge) through
// the ntb_seg kernel module (https://github.com/oguzhankynk/ntb-lab).
//
// One NTB link joins two nodes. ntb_seg gives each side one local memory
// region that the peer writes through its memory window, a mapping of the
// peer's region, doorbells and a DMA engine.
//
// Processes. Up to `processes` (N, the same on both nodes) processes per node
// share the device. The local region and the DMA staging buffer are cut into
// N equal partitions; a process claims the first free one with a lock file
// and uses only that. Partition p of a node is laid out as:
//
//   [0, 4 KiB)        header: magic, version and generation, owner, N and the
//                     partition size, and one credit word per peer partition
//                     q, written by q (how far it read our notices)
//   [4 KiB, 8 KiB)    directory: key, exported flag, offset, size and an
//                     allocation number per local segment (the peer reads it)
//   [8 KiB, ...)      N mailboxes: mailbox q holds 64 slots that peer
//                     partition q fills with interrupts and connect/disconnect
//                     notices, then rings doorbell p
//   [heap, end)       segment memory (from 16 KiB, page aligned)
//
// A writer finds the owner of a segment by scanning the peer's partition
// headers for its endpoint. ntb_seg gives every open file every event, so
// each process sees the doorbells; it reads only its own mailboxes.
//
// The data path is a plain store into the mapped window (or a DMA copy).
//
// Sessions. Every start picks a new generation. An import remembers the
// owner's generation and ntb_seg's link generation; if either changes (the
// owner restarted, the link went down) the import is dead and notify()
// returns kDisconnected. Mailbox slots carry the sender's generation and the
// credit word carries the generation of the sender it counts for, so
// nothing from an older session is ever taken for current.
//
// Threads. The reader thread drains the mailbox, publishes credit first and
// then calls into the core, so a handler that notifies the peer cannot
// starve the peer of credit. No lock is held while waiting.
//
// Limits: N <= 16 and at most the number of doorbells; N > 1 needs an
// ntb_seg that gives every file every event. Remote accesses by the backend are
// 8-byte loads and stores; the core's own writes use memcpy, so on a trapped
// MMIO window (qemu-ntb without fast-mw, ntbd) set dma_threshold = 0.

#include "pushseg/backend.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <thread>
#include <tuple>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include "backends/posix_common.hpp"

namespace pushseg::backends {
namespace {

using backend::Endpoint;
using backend::MappedHost;
using backend::Options;
using backend::RemoteSegment;

// ---- ntb_seg ABI version 1 (ntb-lab include/uapi/linux/ntb_seg.h) --------

struct SegInfo {
  std::uint32_t abi_version;
  std::uint32_t flags;
  std::uint64_t local_size;
  std::uint64_t remote_size;
  std::uint64_t staging_size;
  std::uint32_t db_count;
  std::uint32_t link_gen;
};
struct SegDma {
  std::uint64_t src_off;
  std::uint64_t dst_off;
  std::uint64_t len;
};
struct SegEvent {
  std::uint32_t type;
  std::uint32_t link_gen;
  std::uint64_t value;
};
constexpr std::size_t kSegInfoBytes = 40;
constexpr std::size_t kSegDmaBytes = 24;
constexpr std::size_t kSegEventBytes = 16;
static_assert(sizeof(SegInfo) == kSegInfoBytes && sizeof(SegDma) == kSegDmaBytes &&
                  sizeof(SegEvent) == kSegEventBytes,
              "ntb_seg ABI");

constexpr std::uint32_t kAbiVersion = 1;
constexpr std::uint64_t kMmapLocal = 0x00000000;
constexpr std::uint64_t kMmapRemote = 0x20000000;
constexpr std::uint64_t kMmapStaging = 0x60000000;
constexpr std::uint32_t kLinkUp = 1U << 0U;
constexpr std::uint32_t kPeerReady = 1U << 1U;
constexpr std::uint32_t kHasDma = 1U << 2U;
constexpr std::uint32_t kSharedEvents = 1U << 3U;
constexpr std::uint32_t kEvLink = 2;
constexpr char kIocType = 'N';
const unsigned long kIocInfo = _IOR(kIocType, 0x40, SegInfo);        // NOLINT
const unsigned long kIocRing = _IOW(kIocType, 0x41, std::uint64_t);  // NOLINT
const unsigned long kIocFlush = _IO(kIocType, 0x42);                 // NOLINT
const unsigned long kIocDmaWrite = _IOW(kIocType, 0x43, SegDma);     // NOLINT
const unsigned long kIocDmaWait = _IO(kIocType, 0x44);               // NOLINT

// ---- layout of the local region ------------------------------------------

constexpr std::uint64_t kMagic = 0x42544E4753555050ULL;  // "PUSGNTB" + version byte
constexpr std::uint32_t kVersion = 3;
constexpr std::size_t kMaxProcs = 16;
constexpr std::size_t kWord = 8;
constexpr std::size_t kDirOff = 4096;
constexpr std::size_t kDirEntries = 128;
constexpr std::size_t kMboxOff = 8192;
constexpr std::size_t kSlots = 64;
constexpr std::size_t kMinHeapOff = 16384;
constexpr std::size_t kAlign = 4096;
constexpr std::size_t kDmaAlign = 64;
constexpr unsigned kHi = 32;  // upper half of a word
constexpr std::uint64_t kLo32 = 0xffffffffULL;

// Header words.
constexpr std::size_t kHdrMagic = 0 * kWord;  // kMagic
constexpr std::size_t kHdrGen = 1 * kWord;    // version << 32 | generation
constexpr std::size_t kHdrOwner = 2 * kWord;  // packed Endpoint (owner_word)
constexpr std::size_t kHdrProcs = 3 * kWord;  // N
constexpr std::size_t kHdrPart = 4 * kWord;   // partition size in bytes
constexpr std::size_t kHdrCredit =
    8 * kWord;  // [q], by peer partition q: our gen << 32 | read count

// Directory entry (4 words): key | exported << 32, offset, size, allocation number.
constexpr std::size_t kDirEntryBytes = 4 * kWord;
// Mailbox slot (4 words): seq, key | sender gen << 32, imm, packed type and sender.
constexpr std::size_t kSlotBytes = 4 * kWord;
constexpr std::size_t kMboxBytes = kSlots * kSlotBytes;

// Packing of the owner word and a slot's last word.
constexpr unsigned kNodeShift = 0;
constexpr unsigned kAppShift = 16;
constexpr unsigned kInstShift = 24;
constexpr unsigned kSlotSenderShift = 8;  // the slot's type is in the low byte
constexpr std::uint64_t kByte = 0xffU;
constexpr std::uint64_t kNodeMask = 0xffffU;

enum class SlotType : std::uint8_t { kInterrupt = 1, kConnect, kDisconnect };

std::uint64_t load64(const volatile std::uint8_t* base, std::size_t off) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
  return __atomic_load_n(reinterpret_cast<const volatile std::uint64_t*>(base + off),
                         __ATOMIC_ACQUIRE);
}

void store64(volatile std::uint8_t* base, std::size_t off, std::uint64_t v) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
  __atomic_store_n(reinterpret_cast<volatile std::uint64_t*>(base + off), v, __ATOMIC_RELEASE);
}

// Write-combined stores leave the CPU before later ones.
void sfence() {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_sfence();
#else
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
#endif
}

std::uint64_t pack_peer(NodeId node, AppId app, InstId inst) {
  return (static_cast<std::uint64_t>(node) << kNodeShift) |
         (static_cast<std::uint64_t>(app) << kAppShift) |
         (static_cast<std::uint64_t>(inst) << kInstShift);
}

PeerId unpack_peer(std::uint64_t w) {
  return PeerId{static_cast<NodeId>((w >> kNodeShift) & kNodeMask),
                static_cast<AppId>((w >> kAppShift) & kByte),
                static_cast<InstId>((w >> kInstShift) & kByte)};
}

// An imported segment.
struct Import {
  std::size_t part = 0;  // the owner's partition
  std::uint32_t key = 0;
  std::uint32_t owner_gen = 0;  // the owner's generation when we imported
  std::uint32_t link_gen = 0;   // ntb_seg's link generation then
  std::size_t off = 0;          // offset in the owner's partition
};

struct Importer {
  std::uint32_t key;
  PeerId peer;
  bool operator<(const Importer& o) const {
    return std::tie(key, peer.node, peer.app, peer.inst) <
           std::tie(o.key, o.peer.node, o.peer.app, o.peer.inst);
  }
};

// One notice taken from the mailbox, delivered after the credit is out.
struct Notice {
  SlotType type;
  std::uint32_t key;
  PeerId from;
  std::uint64_t imm;
};

class NtbBackend final : public backend::MappedBackend {
public:
  NtbBackend() = default;
  NtbBackend(const NtbBackend&) = delete;
  NtbBackend& operator=(const NtbBackend&) = delete;
  NtbBackend(NtbBackend&&) = delete;
  NtbBackend& operator=(NtbBackend&&) = delete;
  ~NtbBackend() override {
    shutdown();
  }

  Status start(const Endpoint& self, const Options& opts, MappedHost& host) override {
    self_ = self;
    host_ = &host;
    const std::string dev = posix::opt(opts, "device", "/dev/ntb_seg0");
    const long start_ms = posix::opt_long(opts, "start_timeout_ms", 30000);  // NOLINT
    irq_timeout_ms_ = posix::opt_long(opts, "interrupt_timeout_ms", 5000);   // NOLINT
    const long procs = posix::opt_long(opts, "processes", 1);
    if (procs < 1 || procs > static_cast<long>(kMaxProcs)) {
      return Status::kInvalidArgument;
    }
    procs_ = static_cast<std::size_t>(procs);
    // a partition of our own: the first whose lock file is free
    const std::string lock = posix::opt(opts, "lock_dir", "/tmp") + "/pushseg.ntb." +
                             dev.substr(dev.rfind('/') + 1) + ".";
    Status st = Status::kExists;
    for (part_ = 0; part_ < procs_; ++part_) {
      st = lock_.acquire(lock + std::to_string(part_) + ".lock");
      if (st != Status::kExists) {
        break;
      }
    }
    if (st != Status::kOk) {
      return st;  // kExists: all partitions are taken
    }
    fd_ = ::open(dev.c_str(), O_RDWR | O_CLOEXEC | O_NONBLOCK);
    if (fd_ < 0) {
      shutdown();
      return Status::kIoError;
    }
    // The local region exists once the NTB link has been up once.
    SegInfo info{};
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(start_ms);
    while (get_info(info) && info.local_size == 0) {
      if (std::chrono::steady_clock::now() > end) {
        shutdown();
        return Status::kTimeout;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));  // NOLINT
    }
    heap_off_ =
        std::max(kMinHeapOff, (kMboxOff + procs_ * kMboxBytes + kAlign - 1) / kAlign * kAlign);
    const std::size_t lpart = info.local_size / procs_ / kAlign * kAlign;
    if (info.abi_version != kAbiVersion || lpart < heap_off_ + kAlign ||
        (procs_ > 1 && ((info.flags & kSharedEvents) == 0 || info.db_count < procs_))) {
      shutdown();
      return Status::kUnsupported;
    }
    void* p = ::mmap(nullptr,
                     info.local_size,
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED,
                     fd_,
                     static_cast<off_t>(kMmapLocal));
    if (p == MAP_FAILED) {
      shutdown();
      return Status::kIoError;
    }
    local_map_ = static_cast<std::uint8_t*>(p);
    local_map_size_ = info.local_size;
    local_ = local_map_ + part_ * lpart;  // NOLINT
    local_size_ = lpart;
    if (info.staging_size != 0) {
      p = ::mmap(nullptr,
                 info.staging_size,
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED,
                 fd_,
                 static_cast<off_t>(kMmapStaging));
      if (p != MAP_FAILED) {
        staging_map_ = static_cast<std::uint8_t*>(p);
        staging_map_size_ = info.staging_size;
        staging_size_ = info.staging_size / procs_ / kDmaAlign * kDmaAlign;
        staging_base_ = part_ * staging_size_;
      }
    }
    has_dma_ = (info.flags & kHasDma) != 0 && staging_map_ != nullptr && staging_size_ != 0;

    // A fresh partition: everything a previous process left is void.
    store64(local_, kHdrMagic, 0);
    std::memset(local_ + kHdrProcs, 0, heap_off_ - kHdrProcs);  // NOLINT
    gen_ = (static_cast<std::uint32_t>(std::time(nullptr)) ^
            (static_cast<std::uint32_t>(::getpid()) << kAppShift)) |
           1U;
    store64(local_, kHdrProcs, procs_);
    store64(local_, kHdrPart, local_size_);
    store64(local_, kHdrOwner, pack_peer(self.node, self.app, self.inst));
    store64(local_, kHdrGen, (static_cast<std::uint64_t>(kVersion) << kHi) | gen_);
    store64(local_, kHdrMagic, kMagic);  // last: the header is complete
    free_.emplace(heap_off_, local_size_ - heap_off_);
    ready_ = (info.flags & (kLinkUp | kPeerReady)) == (kLinkUp | kPeerReady);

    wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake_fd_ < 0) {
      shutdown();
      return Status::kIoError;
    }
    reader_ = std::thread([this] { event_loop(); });
    return Status::kOk;
  }

  Status alloc_segment(std::uint32_t key, std::size_t size, void** base) override {
    std::lock_guard<std::mutex> lk(mu_);
    if (local_ == nullptr) {
      return Status::kIoError;
    }
    if (segs_.count(key) != 0) {
      return Status::kExists;
    }
    const std::size_t len = (size + kAlign - 1) / kAlign * kAlign;
    std::size_t slot = kDirEntries;
    for (std::size_t i = 0; i < kDirEntries && slot == kDirEntries; ++i) {
      if (load64(local_, dir_off(i) + 2 * kWord) == 0) {  // size 0: unused
        slot = i;
      }
    }
    std::size_t off = 0;
    for (auto it = free_.begin(); it != free_.end(); ++it) {  // first fit
      if (it->second >= len) {
        off = it->first;
        const std::size_t rest = it->second - len;
        free_.erase(it);
        if (rest != 0) {
          free_.emplace(off + len, rest);
        }
        break;
      }
    }
    if (off == 0 || slot == kDirEntries) {
      if (off != 0) {
        release_range(off, len);
      }
      return Status::kIoError;  // region or directory full
    }
    std::memset(local_ + off, 0, len);  // NOLINT
    // the peer reads entries without a lock: key word last, allocation number first
    store64(local_, dir_off(slot) + 3 * kWord, ++alloc_no_);
    store64(local_, dir_off(slot) + kWord, off);
    store64(local_, dir_off(slot) + 2 * kWord, size);
    store64(local_, dir_off(slot), key);  // not exported yet
    segs_[key] = Local{off, len, slot};
    *base = local_ + off;  // NOLINT
    return Status::kOk;
  }

  Status export_segment(std::uint32_t key) override {
    return set_exported(key, true) ? Status::kOk : Status::kNotFound;
  }

  void unexport_segment(std::uint32_t key) override {
    (void)set_exported(key, false);
  }

  void free_segment(std::uint32_t key) override {
    std::lock_guard<std::mutex> lk(mu_);
    const auto it = segs_.find(key);
    if (it == segs_.end()) {
      return;
    }
    if (local_ != nullptr) {
      store64(local_, dir_off(it->second.slot), 0);  // new imports stop here
      store64(local_, dir_off(it->second.slot) + 2 * kWord, 0);
      if (has_importer_locked(key)) {
        // a peer may still write there: keep the memory until it lets go
        quarantine_.emplace(key, it->second);
      } else {
        release_range(it->second.off, it->second.len);
      }
    }
    segs_.erase(it);
  }

  Status import_segment(const Endpoint& remote, std::uint32_t key, RemoteSegment& out) override {
    // counted from the start: ensure_remote() must not replace the mapping
    // between reading it and handing out a pointer into it
    n_imports_.fetch_add(1);
    const Status st = import_locked_count(remote, key, out);
    if (st != Status::kOk) {
      n_imports_.fetch_sub(1);
    }
    return st;
  }

  Status import_locked_count(const Endpoint& remote, std::uint32_t key, RemoteSegment& out) {
    SegInfo info{};
    if (!get_info(info) || (info.flags & kPeerReady) == 0 || !ensure_remote(info)) {
      return Status::kNotFound;  // peer not loaded or not ready yet
    }
    auto imp = std::make_unique<Import>();
    std::uint64_t size = 0;
    {
      std::shared_lock<std::shared_mutex> lk(map_mu_);
      const std::uint64_t owner = pack_peer(remote.node, remote.app, remote.inst);
      std::size_t q = 0;
      for (; q < procs_; ++q) {
        if (load64(rpart(q), kHdrMagic) == kMagic && load64(rpart(q), kHdrOwner) == owner) {
          break;
        }
      }
      if (q == procs_) {
        return Status::kNotFound;  // the owner has not started (yet)
      }
      volatile std::uint8_t* hdr = rpart(q);
      const std::uint64_t genw = load64(hdr, kHdrGen);
      if ((genw >> kHi) != kVersion || load64(hdr, kHdrProcs) != procs_ ||
          load64(hdr, kHdrPart) != rpart_size_) {
        return Status::kProtocolError;  // another layout: "processes" or seg_size differ
      }
      const std::size_t heap = heap_off_;  // the same N, so the same layout
      std::size_t i = 0;
      for (; i < kDirEntries; ++i) {
        const std::uint64_t w0 = load64(hdr, dir_off(i));
        if ((w0 & kLo32) != key || (w0 >> kHi) == 0) {
          continue;
        }
        const std::uint64_t no = load64(hdr, dir_off(i) + 3 * kWord);
        const std::uint64_t off = load64(hdr, dir_off(i) + kWord);
        size = load64(hdr, dir_off(i) + 2 * kWord);
        // the owner may have freed and reused the entry meanwhile
        if (load64(hdr, dir_off(i)) != w0 || load64(hdr, dir_off(i) + 3 * kWord) != no) {
          return Status::kNotFound;
        }
        if (off < heap || size == 0 || off > rpart_size_ || size > rpart_size_ - off) {
          return Status::kProtocolError;
        }
        imp->off = off;
        break;
      }
      if (i == kDirEntries) {
        return Status::kNotFound;
      }
      imp->part = q;
      imp->key = key;
      imp->owner_gen = static_cast<std::uint32_t>(genw & kLo32);
      imp->link_gen = info.link_gen;
      out.base = hdr + imp->off;  // NOLINT
    }
    // the owner must know we write there, before we do (see free_segment)
    if (send(*imp, SlotType::kConnect, 0) != Status::kOk) {
      out = RemoteSegment{};
      return Status::kNotFound;
    }
    out.size = size;
    out.handle = imp.release();
    return Status::kOk;
  }

  void release_segment(RemoteSegment& seg) override {
    std::unique_ptr<Import> imp(static_cast<Import*>(seg.handle));
    if (imp == nullptr) {
      return;
    }
    (void)send(*imp, SlotType::kDisconnect, 0);  // fails at once if the owner is gone
    n_imports_.fetch_sub(1);
    seg = RemoteSegment{};
  }

  Status flush(const RemoteSegment& seg) override {
    store_barrier(seg);
    return ::ioctl(fd_, kIocFlush) == 0 ? Status::kOk : Status::kDisconnected;  // NOLINT
  }

  Status interrupt(const RemoteSegment& seg, std::uint64_t imm) override {
    return send(*static_cast<const Import*>(seg.handle), SlotType::kInterrupt, imm);
  }

  [[nodiscard]] bool has_dma() const override {
    return has_dma_;
  }

  Status dma_write(const RemoteSegment& seg,
                   std::uint64_t off,
                   const void* src,
                   std::size_t len) override {
    const auto* imp = static_cast<const Import*>(seg.handle);
    const auto* s = static_cast<const std::uint8_t*>(src);
    std::lock_guard<std::mutex> lk(dma_mu_);
    if (staging_map_ == nullptr) {
      return Status::kDisconnected;
    }
    while (len != 0) {
      const std::size_t n = std::min(len, staging_size_);
      if (staging_pos_ + n > staging_size_) {  // staging full: let it drain
        if (::ioctl(fd_, kIocDmaWait) != 0) {  // NOLINT
          return Status::kDisconnected;
        }
        staging_pos_ = 0;
      }
      std::memcpy(staging_map_ + staging_base_ + staging_pos_, s, n);  // NOLINT
      SegDma d{staging_base_ + staging_pos_, imp->part * rpart_size_ + imp->off + off, n};
      if (::ioctl(fd_, kIocDmaWrite, &d) != 0) {  // NOLINT
        return errno == EINVAL ? Status::kOutOfRange : Status::kDisconnected;
      }
      staging_pos_ = (staging_pos_ + n + kDmaAlign - 1) / kDmaAlign * kDmaAlign;
      s += n;  // NOLINT
      off += n;
      len -= n;
    }
    return Status::kOk;
  }

  Status dma_wait(const RemoteSegment& /*seg*/) override {
    std::lock_guard<std::mutex> lk(dma_mu_);
    staging_pos_ = 0;
    return ::ioctl(fd_, kIocDmaWait) == 0 ? Status::kOk : Status::kDisconnected;  // NOLINT
  }

  void stop() override {
    shutdown();
  }

private:
  struct Local {
    std::size_t off = 0;
    std::size_t len = 0;
    std::size_t slot = 0;
  };

  static std::size_t dir_off(std::size_t i) {
    return kDirOff + i * kDirEntryBytes;
  }

  // Slot @seq of mailbox @from (the sender's partition).
  static std::size_t slot_off(std::size_t from, std::uint64_t seq) {
    return kMboxOff + from * kMboxBytes + (seq % kSlots) * kSlotBytes;
  }

  // Under map_mu_: the peer's partition @q.
  volatile std::uint8_t* rpart(std::size_t q) const {
    return remote_ + q * rpart_size_;  // NOLINT
  }

  bool get_info(SegInfo& info) const {
    return ::ioctl(fd_, kIocInfo, &info) == 0;  // NOLINT
  }

  // Maps the peer's region. A mapping in use by an import is never replaced
  // (the core holds pointers into it); a different size waits until then.
  bool ensure_remote(const SegInfo& info) {
    if (info.remote_size / procs_ < heap_off_ + kAlign) {
      return false;
    }
    {
      std::shared_lock<std::shared_mutex> lk(map_mu_);
      if (remote_ != nullptr && (remote_size_ == info.remote_size || n_imports_.load() != 0)) {
        return true;
      }
    }
    std::unique_lock<std::shared_mutex> lk(map_mu_);
    if (remote_ != nullptr && (remote_size_ == info.remote_size || n_imports_.load() != 0)) {
      return true;  // (an import in progress counts: it may hold a pointer already)
    }
    if (remote_ != nullptr) {
      ::munmap(const_cast<std::uint8_t*>(remote_), remote_size_);  // NOLINT
    }
    void* p = ::mmap(nullptr,
                     info.remote_size,
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED,
                     fd_,
                     static_cast<off_t>(kMmapRemote));
    remote_ = p == MAP_FAILED ? nullptr : static_cast<volatile std::uint8_t*>(p);
    remote_size_ = remote_ == nullptr ? 0 : info.remote_size;
    rpart_size_ = remote_size_ / procs_ / kAlign * kAlign;
    return remote_ != nullptr;
  }

  // Under map_mu_ (shared). Is the owner of @imp still the one we imported from?
  bool session_alive(const Import& imp) const {
    SegInfo info{};
    return remote_ != nullptr && get_info(info) && (info.flags & kPeerReady) != 0 &&
           info.link_gen == imp.link_gen && load64(rpart(imp.part), kHdrMagic) == kMagic &&
           (load64(rpart(imp.part), kHdrGen) & kLo32) == imp.owner_gen;
  }

  // How many of our notices the owner has not read yet. The credit word may
  // be from an older session; then it counts as "nothing read".
  std::uint64_t unread(std::size_t q) const {
    const std::uint64_t c = load64(local_, kHdrCredit + q * kWord);
    const std::uint64_t read = (c >> kHi) == gen_ ? (c & kLo32) : 0;
    return static_cast<std::uint32_t>(static_cast<std::uint32_t>(tx_.at(q).seq) - read);
  }

  // One slot in our mailbox at the owner, then the owner's doorbell. Waits
  // (unlocked for everyone but other senders to that partition) while the
  // mailbox is full: notices are never dropped.
  Status send(const Import& imp, SlotType type, std::uint64_t imm) {
    const std::size_t q = imp.part;
    Tx& t = tx_.at(q);
    std::lock_guard<std::mutex> tx(t.mu);
    const std::uint64_t session = (static_cast<std::uint64_t>(imp.owner_gen) << kHi) | imp.link_gen;
    if (local_ == nullptr) {
      return Status::kDisconnected;
    }
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(irq_timeout_ms_);
    while (true) {
      {
        std::shared_lock<std::shared_mutex> lk(map_mu_);
        if (!session_alive(imp)) {
          return Status::kDisconnected;  // and leaves the live session's state alone
        }
        if (session != t.session) {  // a new owner or a new link: its mailbox is empty
          t.session = session;
          t.seq = 0;
          store64(local_, kHdrCredit + q * kWord, static_cast<std::uint64_t>(gen_) << kHi);
        }
        if (unread(q) < kSlots) {
          volatile std::uint8_t* owner = rpart(q);
          const std::size_t o = slot_off(part_, t.seq);
          store64(owner, o + kWord, imp.key | (static_cast<std::uint64_t>(gen_) << kHi));
          store64(owner, o + 2 * kWord, imm);
          store64(owner,
                  o + 3 * kWord,
                  static_cast<std::uint64_t>(type) |
                      (pack_peer(self_.node, self_.app, self_.inst) << kSlotSenderShift));
          sfence();
          store64(owner, o, ++t.seq);  // the slot is valid from here
          break;
        }
      }
      if (std::chrono::steady_clock::now() > end) {
        return Status::kDisconnected;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(100));  // NOLINT
    }
    const std::uint64_t bell = 1ULL << q;
    return ::ioctl(fd_, kIocRing, &bell) == 0 ? Status::kOk : Status::kDisconnected;  // NOLINT
  }

  bool set_exported(std::uint32_t key, bool on) {
    std::lock_guard<std::mutex> lk(mu_);
    const auto it = segs_.find(key);
    if (it == segs_.end() || local_ == nullptr) {
      return false;
    }
    store64(local_, dir_off(it->second.slot), key | (static_cast<std::uint64_t>(on) << kHi));
    return true;
  }

  // Under mu_.
  bool has_importer_locked(std::uint32_t key) const {
    const auto it = importers_.lower_bound(Importer{key, PeerId{}});
    return it != importers_.end() && it->key == key;
  }

  // Under mu_: return [off, off + len) to the free list, merging neighbours.
  void release_range(std::size_t off, std::size_t len) {
    auto next = free_.lower_bound(off);
    if (next != free_.end() && off + len == next->first) {
      len += next->second;
      next = free_.erase(next);
    }
    if (next != free_.begin()) {
      auto prev = std::prev(next);
      if (prev->first + prev->second == off) {
        prev->second += len;
        return;
      }
    }
    free_.emplace(off, len);
  }

  // Reader thread: take the notices waiting in mailbox @j, in order. A
  // sender that restarted begins again at slot 0 with a new generation.
  void take_notices(std::size_t j, std::vector<Notice>& out, bool& sender_restarted) {
    Rx& x = rx_.at(j);
    while (true) {
      const std::size_t o = slot_off(j, x.seq);
      const std::uint64_t seq = load64(local_, o);
      const std::uint64_t w1 = load64(local_, o + kWord);
      const auto g = static_cast<std::uint32_t>(w1 >> kHi);
      if (seq == x.seq + 1 && g != 0 && (x.gen == 0 || g == x.gen)) {
        const std::uint64_t imm = load64(local_, o + 2 * kWord);
        const std::uint64_t w3 = load64(local_, o + 3 * kWord);
        out.push_back(Notice{static_cast<SlotType>(w3 & kByte),
                             static_cast<std::uint32_t>(w1 & kLo32),
                             unpack_peer(w3 >> kSlotSenderShift),
                             imm});
        x.gen = g;
        ++x.seq;
        continue;
      }
      const std::size_t o0 = slot_off(j, 0);
      const std::uint64_t s0 = load64(local_, o0);
      const auto g0 = static_cast<std::uint32_t>(load64(local_, o0 + kWord) >> kHi);
      if (s0 == 1 && g0 != 0 && g0 != x.gen) {
        sender_restarted = sender_restarted || x.gen != 0;
        x.seq = 0;
        x.gen = g0;
        continue;
      }
      return;
    }
  }

  // Reader thread: per mailbox, credit first, then the core's callbacks.
  void drain_mailboxes() {
    bool mapped = false;
    // a handler may stop the backend (shutdown() from this thread): then stop
    for (std::size_t j = 0; j < procs_ && !stopped(); ++j) {
      std::vector<Notice> notices;
      bool restarted = false;
      take_notices(j, notices, restarted);
      Rx& x = rx_.at(j);
      if (x.gen != 0) {
        if (!mapped) {
          SegInfo info{};
          if (get_info(info) && (info.flags & kPeerReady) != 0) {
            (void)ensure_remote(info);
          }
          mapped = true;
        }
        std::shared_lock<std::shared_mutex> lk(map_mu_);
        if (remote_ != nullptr) {
          store64(rpart(j),
                  kHdrCredit + part_ * kWord,
                  (static_cast<std::uint64_t>(x.gen) << kHi) | (x.seq & kLo32));
          sfence();
        }
      }
      if (restarted && x.has_peer) {
        lose_importers(&x.peer);  // the old process's imports died with it
      }
      for (const Notice& n : notices) {
        if (stopped()) {
          return;
        }
        moved_from_other_mailbox(j, n.from);
        x.peer = n.from;
        x.has_peer = true;
        deliver(n);
      }
    }
  }

  bool stopped() const {
    return stopping_.load(std::memory_order_acquire);
  }

  // Reader thread: an endpoint lives in one partition at a time. If @who now
  // writes from mailbox @j but used another one before, its old process
  // died and it came back elsewhere: its old imports are gone.
  void moved_from_other_mailbox(std::size_t j, const PeerId& who) {
    for (std::size_t i = 0; i < procs_; ++i) {
      Rx& o = rx_.at(i);
      if (i != j && o.has_peer && o.peer.node == who.node && o.peer.app == who.app &&
          o.peer.inst == who.inst) {
        o.has_peer = false;
        lose_importers(&who);
      }
    }
  }

  void deliver(const Notice& n) {
    switch (n.type) {
      case SlotType::kInterrupt:
        host_->on_interrupt(n.key, n.from, n.imm);
        break;
      case SlotType::kConnect: {
        std::lock_guard<std::mutex> lk(mu_);
        importers_.insert(Importer{n.key, n.from});
      }
        host_->on_peer_connect(n.key, n.from);
        break;
      case SlotType::kDisconnect: {
        std::lock_guard<std::mutex> lk(mu_);
        importers_.erase(Importer{n.key, n.from});
        release_quarantine_locked(n.key);
      }
        host_->on_peer_disconnect(n.key, n.from, false);
        break;
      default:
        break;  // unknown: a newer peer
    }
  }

  // Under mu_: a freed segment's memory goes back once nobody imports it.
  void release_quarantine_locked(std::uint32_t key) {
    if (has_importer_locked(key)) {
      return;
    }
    const auto range = quarantine_.equal_range(key);
    for (auto q = range.first; q != range.second; ++q) {
      release_range(q->second.off, q->second.len);
    }
    quarantine_.erase(range.first, range.second);
  }

  // Reader thread: a peer process (@who) or, with nullptr, the link went away.
  void lose_importers(const PeerId* who) {
    std::set<Importer> lost;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (who == nullptr) {
        lost.swap(importers_);
      } else {
        for (auto it = importers_.begin(); it != importers_.end();) {
          if (it->peer.node == who->node && it->peer.app == who->app &&
              it->peer.inst == who->inst) {
            lost.insert(*it);
            it = importers_.erase(it);
          } else {
            ++it;
          }
        }
      }
      for (auto q = quarantine_.begin(); q != quarantine_.end();) {
        if (!has_importer_locked(q->first)) {
          release_range(q->second.off, q->second.len);
          q = quarantine_.erase(q);
        } else {
          ++q;
        }
      }
    }
    for (const Importer& i : lost) {
      host_->on_peer_disconnect(i.key, i.peer, true);
    }
  }

  void link_lost() {
    lose_importers(nullptr);
    if (stopped()) {
      return;  // a handler stopped the backend: the region is gone
    }
    rx_.fill(Rx{});
    std::memset(local_ + kMboxOff, 0, procs_ * kMboxBytes);  // NOLINT
  }

  void event_loop() {
    while (!stopping_.load(std::memory_order_acquire)) {
      pollfd fds[2] = {{fd_, POLLIN, 0}, {wake_fd_, POLLIN, 0}};  // NOLINT
      if (::poll(fds, 2, 200) < 0 && errno != EINTR) {            // NOLINT
        return;
      }
      if (stopping_.load(std::memory_order_acquire)) {
        return;
      }
      if ((fds[0].revents & (POLLHUP | POLLERR)) != 0) {
        link_lost();  // the NTB device is gone
        return;
      }
      SegEvent ev{};
      ssize_t n = 0;
      while ((n = ::read(fd_, &ev, sizeof(ev))) == static_cast<ssize_t>(sizeof(ev))) {
        if (ev.type == kEvLink) {
          const bool now = (ev.value & (kLinkUp | kPeerReady)) == (kLinkUp | kPeerReady);
          if (ready_ && !now) {
            link_lost();
          }
          ready_ = now;
        }
        // doorbell events (type 1): drained below, merged ones included
      }
      if (n == 0) {
        link_lost();  // device removed
        return;
      }
      drain_mailboxes();
    }
  }

  void shutdown() {
    if (stopping_.exchange(true)) {
      return;
    }
    if (wake_fd_ >= 0) {
      const std::uint64_t one = 1;
      (void)!::write(wake_fd_, &one, sizeof(one));
    }
    if (reader_.joinable()) {
      if (reader_.get_id() == std::this_thread::get_id()) {
        reader_.detach();  // a handler stopped the context: we are the reader
      } else {
        reader_.join();
      }
    }
    std::vector<std::unique_lock<std::mutex>> txl;
    for (Tx& t : tx_) {
      txl.emplace_back(t.mu);  // cppcheck-suppress useStlAlgorithm
    }
    std::scoped_lock lk(mu_, dma_mu_);
    std::unique_lock<std::shared_mutex> mlk(map_mu_);
    if (local_ != nullptr) {
      store64(local_, kHdrMagic, 0);  // the peer's next import or notify fails
      local_ = nullptr;
    }
    if (local_map_ != nullptr) {
      ::munmap(local_map_, local_map_size_);
      local_map_ = nullptr;
    }
    if (remote_ != nullptr) {
      ::munmap(const_cast<std::uint8_t*>(remote_), remote_size_);  // NOLINT
      remote_ = nullptr;
    }
    if (staging_map_ != nullptr) {
      ::munmap(staging_map_, staging_map_size_);
      staging_map_ = nullptr;
    }
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
    if (wake_fd_ >= 0) {
      ::close(wake_fd_);
      wake_fd_ = -1;
    }
    lock_.release();
  }

  Endpoint self_;
  MappedHost* host_ = nullptr;
  posix::IdentityLock lock_;
  int fd_ = -1;
  int wake_fd_ = -1;
  long irq_timeout_ms_ = 5000;  // NOLINT
  std::uint32_t gen_ = 0;
  std::atomic<bool> stopping_{false};
  std::thread reader_;

  std::size_t procs_ = 1;  // partitions per node (N)
  std::size_t part_ = 0;   // ours
  std::size_t heap_off_ = kMinHeapOff;

  // Lock order: a Tx::mu, then map_mu_; mu_ and dma_mu_ are taken alone.
  std::mutex mu_;                      // local segments, directory, importers
  std::uint8_t* local_map_ = nullptr;  // the whole local region
  std::size_t local_map_size_ = 0;
  std::uint8_t* local_ = nullptr;  // our partition of it
  std::size_t local_size_ = 0;
  std::map<std::uint32_t, Local> segs_;
  std::multimap<std::uint32_t, Local> quarantine_;  // freed, but still imported
  std::map<std::size_t, std::size_t> free_;         // offset -> length
  std::set<Importer> importers_;
  std::uint64_t alloc_no_ = 0;

  std::shared_mutex map_mu_;  // the peer's region mapping
  volatile std::uint8_t* remote_ = nullptr;
  std::size_t remote_size_ = 0;
  std::size_t rpart_size_ = 0;
  std::atomic<long> n_imports_{0};

  // Our notices into the mailbox of each peer partition.
  struct Tx {
    std::mutex mu;
    std::uint64_t seq = 0;
    std::uint64_t session = 0;
  };
  std::array<Tx, kMaxProcs> tx_;

  std::mutex dma_mu_;
  std::uint8_t* staging_map_ = nullptr;  // the whole staging buffer
  std::size_t staging_map_size_ = 0;
  std::size_t staging_base_ = 0;  // our part of it
  std::size_t staging_size_ = 0;
  std::size_t staging_pos_ = 0;
  bool has_dma_ = false;

  // reader thread only
  bool ready_ = false;
  struct Rx {
    std::uint64_t seq = 0;
    std::uint32_t gen = 0;  // generation of the sender we are reading
    PeerId peer{};          // who sent the last notice
    bool has_peer = false;
  };
  std::array<Rx, kMaxProcs> rx_{};
};

}  // namespace

std::unique_ptr<backend::MappedBackend> make_ntb_backend() {
  return std::make_unique<NtbBackend>();
}

}  // namespace pushseg::backends
