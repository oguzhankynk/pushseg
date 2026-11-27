// Built-in mapped backend "ntb": PCIe NTB (non-transparent bridge) through
// the ntb_seg kernel module (https://github.com/oguzhankynk/ntb-lab).
//
// One NTB link joins two nodes. ntb_seg gives each side one local memory
// region that the peer writes through its memory window, a mapping of the
// peer's region, doorbells and a DMA engine. This backend divides the local
// region:
//
//   [0, 4 KiB)        header: magic, version and generation, owner, and the
//                     credit word the peer writes (how far it read our
//                     notices in its mailbox)
//   [4 KiB, 8 KiB)    directory: key, exported flag, offset, size and an
//                     allocation number per local segment (the peer reads it)
//   [8 KiB, 10 KiB)   mailbox: 64 slots the peer fills with interrupts and
//                     connect/disconnect notices, then rings doorbell 0
//   [16 KiB, end)     segment memory
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
// Limits: one process per NTB device (ntb_seg has one event stream), and so
// one process per node on an NTB link. Remote accesses by the backend are
// 8-byte loads and stores; the core's own writes use memcpy, so on a trapped
// MMIO window (qemu-ntb without fast-mw, ntbd) set dma_threshold = 0.

#include "pushseg/backend.hpp"

#include <algorithm>
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
constexpr std::uint32_t kEvLink = 2;
constexpr char kIocType = 'N';
const unsigned long kIocInfo = _IOR(kIocType, 0x40, SegInfo);        // NOLINT
const unsigned long kIocRing = _IOW(kIocType, 0x41, std::uint64_t);  // NOLINT
const unsigned long kIocFlush = _IO(kIocType, 0x42);                 // NOLINT
const unsigned long kIocDmaWrite = _IOW(kIocType, 0x43, SegDma);     // NOLINT
const unsigned long kIocDmaWait = _IO(kIocType, 0x44);               // NOLINT

// ---- layout of the local region ------------------------------------------

constexpr std::uint64_t kMagic = 0x42544E4753555050ULL;  // "PUSGNTB" + version byte
constexpr std::uint32_t kVersion = 2;
constexpr std::size_t kWord = 8;
constexpr std::size_t kDirOff = 4096;
constexpr std::size_t kDirEntries = 128;
constexpr std::size_t kMboxOff = 8192;
constexpr std::size_t kSlots = 64;
constexpr std::size_t kHeapOff = 16384;
constexpr std::size_t kAlign = 4096;
constexpr std::size_t kDmaAlign = 64;
constexpr std::uint64_t kDoorbell = 1;  // bit 0: "look at the mailbox"
constexpr unsigned kHi = 32;            // upper half of a word
constexpr std::uint64_t kLo32 = 0xffffffffULL;

// Header words.
constexpr std::size_t kHdrMagic = 0 * kWord;   // kMagic
constexpr std::size_t kHdrGen = 1 * kWord;     // version << 32 | generation
constexpr std::size_t kHdrOwner = 2 * kWord;   // packed Endpoint (owner_word)
constexpr std::size_t kHdrCredit = 3 * kWord;  // written by the peer: our gen << 32 | read count

// Directory entry (4 words): key | exported << 32, offset, size, allocation number.
constexpr std::size_t kDirEntryBytes = 4 * kWord;
// Mailbox slot (4 words): seq, key | sender gen << 32, imm, packed type and sender.
constexpr std::size_t kSlotBytes = 4 * kWord;

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
  std::uint32_t key = 0;
  std::uint32_t owner_gen = 0;  // the owner's generation when we imported
  std::uint32_t link_gen = 0;   // ntb_seg's link generation then
  std::size_t off = 0;          // offset in the peer's region
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
    std::string lock = posix::opt(opts, "lock_dir", "/tmp");
    lock += "/pushseg.ntb." + dev.substr(dev.rfind('/') + 1) + ".lock";
    if (const Status st = lock_.acquire(lock); st != Status::kOk) {
      return st;  // another process on this node uses the device
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
    if (info.abi_version != kAbiVersion || info.local_size < kHeapOff + kAlign) {
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
    local_ = static_cast<std::uint8_t*>(p);
    local_size_ = info.local_size;
    if (info.staging_size != 0) {
      p = ::mmap(nullptr,
                 info.staging_size,
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED,
                 fd_,
                 static_cast<off_t>(kMmapStaging));
      if (p != MAP_FAILED) {
        staging_ = static_cast<std::uint8_t*>(p);
        staging_size_ = info.staging_size;
      }
    }
    has_dma_ = (info.flags & kHasDma) != 0 && staging_ != nullptr;

    // A fresh region: everything a previous process left is void.
    std::memset(local_ + kDirOff, 0, kHeapOff - kDirOff);  // NOLINT
    gen_ = (static_cast<std::uint32_t>(std::time(nullptr)) ^
            (static_cast<std::uint32_t>(::getpid()) << kAppShift)) |
           1U;
    store64(local_, kHdrCredit, 0);
    store64(local_, kHdrOwner, pack_peer(self.node, self.app, self.inst));
    store64(local_, kHdrGen, (static_cast<std::uint64_t>(kVersion) << kHi) | gen_);
    store64(local_, kHdrMagic, kMagic);  // last: the header is complete
    free_.emplace(kHeapOff, local_size_ - kHeapOff);
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
      if (load64(remote_, kHdrMagic) != kMagic ||
          unpack_peer(load64(remote_, kHdrOwner)).node != remote.node) {
        return Status::kNotFound;
      }
      const std::uint64_t genw = load64(remote_, kHdrGen);
      if ((genw >> kHi) != kVersion) {
        return Status::kProtocolError;
      }
      std::size_t i = 0;
      for (; i < kDirEntries; ++i) {
        const std::uint64_t w0 = load64(remote_, dir_off(i));
        if ((w0 & kLo32) != key || (w0 >> kHi) == 0) {
          continue;
        }
        const std::uint64_t no = load64(remote_, dir_off(i) + 3 * kWord);
        const std::uint64_t off = load64(remote_, dir_off(i) + kWord);
        size = load64(remote_, dir_off(i) + 2 * kWord);
        // the owner may have freed and reused the entry meanwhile
        if (load64(remote_, dir_off(i)) != w0 || load64(remote_, dir_off(i) + 3 * kWord) != no) {
          return Status::kNotFound;
        }
        if (off < kHeapOff || size == 0 || off > remote_size_ || size > remote_size_ - off) {
          return Status::kProtocolError;
        }
        imp->off = off;
        break;
      }
      if (i == kDirEntries) {
        return Status::kNotFound;
      }
      imp->key = key;
      imp->owner_gen = static_cast<std::uint32_t>(genw & kLo32);
      imp->link_gen = info.link_gen;
      out.base = remote_ + imp->off;  // NOLINT
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
    if (staging_ == nullptr) {
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
      std::memcpy(staging_ + staging_pos_, s, n);  // NOLINT
      SegDma d{staging_pos_, imp->off + off, n};
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

  static std::size_t slot_off(std::uint64_t seq) {
    return kMboxOff + (seq % kSlots) * kSlotBytes;
  }

  bool get_info(SegInfo& info) const {
    return ::ioctl(fd_, kIocInfo, &info) == 0;  // NOLINT
  }

  // Maps the peer's region. A mapping in use by an import is never replaced
  // (the core holds pointers into it); a different size waits until then.
  bool ensure_remote(const SegInfo& info) {
    if (info.remote_size < kHeapOff) {
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
    return remote_ != nullptr;
  }

  // Under map_mu_ (shared). Is the owner of @imp still the one we imported from?
  bool session_alive(const Import& imp) const {
    SegInfo info{};
    return remote_ != nullptr && get_info(info) && (info.flags & kPeerReady) != 0 &&
           info.link_gen == imp.link_gen && load64(remote_, kHdrMagic) == kMagic &&
           (load64(remote_, kHdrGen) & kLo32) == imp.owner_gen;
  }

  // How many of our notices the owner has not read yet. The credit word may
  // be from an older session; then it counts as "nothing read".
  std::uint64_t unread() const {
    const std::uint64_t c = load64(local_, kHdrCredit);
    const std::uint64_t read = (c >> kHi) == gen_ ? (c & kLo32) : 0;
    return static_cast<std::uint32_t>(static_cast<std::uint32_t>(tx_seq_) - read);
  }

  // One mailbox slot, then doorbell 0. Waits (unlocked for everyone but other
  // senders) while the owner's mailbox is full: notices are never dropped.
  Status send(const Import& imp, SlotType type, std::uint64_t imm) {
    std::lock_guard<std::mutex> tx(tx_mu_);
    const std::uint64_t session = (static_cast<std::uint64_t>(imp.owner_gen) << kHi) | imp.link_gen;
    if (local_ == nullptr) {
      return Status::kDisconnected;
    }
    if (session != tx_session_) {  // a new owner or a new link: its mailbox is empty
      tx_session_ = session;
      tx_seq_ = 0;
      store64(local_, kHdrCredit, static_cast<std::uint64_t>(gen_) << kHi);
    }
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(irq_timeout_ms_);
    while (true) {
      {
        std::shared_lock<std::shared_mutex> lk(map_mu_);
        if (!session_alive(imp)) {
          return Status::kDisconnected;
        }
        if (unread() < kSlots) {
          const std::size_t o = slot_off(tx_seq_);
          store64(remote_, o + kWord, imp.key | (static_cast<std::uint64_t>(gen_) << kHi));
          store64(remote_, o + 2 * kWord, imm);
          store64(remote_,
                  o + 3 * kWord,
                  static_cast<std::uint64_t>(type) |
                      (pack_peer(self_.node, self_.app, self_.inst) << kSlotSenderShift));
          sfence();
          store64(remote_, o, ++tx_seq_);  // the slot is valid from here
          break;
        }
      }
      if (std::chrono::steady_clock::now() > end) {
        return Status::kDisconnected;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(100));  // NOLINT
    }
    const std::uint64_t bell = kDoorbell;
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

  // Reader thread: take the notices waiting in our mailbox, in order. A
  // sender that restarted begins again at slot 0 with a new generation.
  void take_notices(std::vector<Notice>& out, bool& sender_restarted) {
    while (true) {
      const std::size_t o = slot_off(rx_seq_);
      const std::uint64_t seq = load64(local_, o);
      const std::uint64_t w1 = load64(local_, o + kWord);
      const auto g = static_cast<std::uint32_t>(w1 >> kHi);
      if (seq == rx_seq_ + 1 && g != 0 && (rx_gen_ == 0 || g == rx_gen_)) {
        const std::uint64_t imm = load64(local_, o + 2 * kWord);
        const std::uint64_t w3 = load64(local_, o + 3 * kWord);
        out.push_back(Notice{static_cast<SlotType>(w3 & kByte),
                             static_cast<std::uint32_t>(w1 & kLo32),
                             unpack_peer(w3 >> kSlotSenderShift),
                             imm});
        rx_gen_ = g;
        ++rx_seq_;
        continue;
      }
      const std::uint64_t s0 = load64(local_, kMboxOff);
      const auto g0 = static_cast<std::uint32_t>(load64(local_, kMboxOff + kWord) >> kHi);
      if (s0 == 1 && g0 != 0 && g0 != rx_gen_) {
        sender_restarted = sender_restarted || rx_gen_ != 0;
        rx_seq_ = 0;
        rx_gen_ = g0;
        continue;
      }
      return;
    }
  }

  // Reader thread: credit first, then the core's callbacks.
  void drain_mailbox() {
    std::vector<Notice> notices;
    bool restarted = false;
    take_notices(notices, restarted);
    if (rx_gen_ != 0) {
      SegInfo info{};
      if (get_info(info) && (info.flags & kPeerReady) != 0) {
        (void)ensure_remote(info);
      }
      std::shared_lock<std::shared_mutex> lk(map_mu_);
      if (remote_ != nullptr) {
        store64(
            remote_, kHdrCredit, (static_cast<std::uint64_t>(rx_gen_) << kHi) | (rx_seq_ & kLo32));
        sfence();
      }
    }
    if (restarted) {
      lose_all_importers();  // the old process's imports died with it
    }
    for (const Notice& n : notices) {
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

  // Reader thread: the peer process or the link went away.
  void lose_all_importers() {
    std::set<Importer> lost;
    {
      std::lock_guard<std::mutex> lk(mu_);
      lost.swap(importers_);
      for (const auto& q : quarantine_) {
        release_range(q.second.off, q.second.len);
      }
      quarantine_.clear();
    }
    for (const Importer& i : lost) {
      host_->on_peer_disconnect(i.key, i.peer, true);
    }
  }

  void link_lost() {
    lose_all_importers();
    rx_seq_ = 0;
    rx_gen_ = 0;
    std::memset(local_ + kMboxOff, 0, kSlots * kSlotBytes);  // NOLINT
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
      drain_mailbox();
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
    std::scoped_lock lk(tx_mu_, mu_, dma_mu_);
    std::unique_lock<std::shared_mutex> mlk(map_mu_);
    if (local_ != nullptr) {
      store64(local_, kHdrMagic, 0);  // the peer's next import or notify fails
      ::munmap(local_, local_size_);
      local_ = nullptr;
    }
    if (remote_ != nullptr) {
      ::munmap(const_cast<std::uint8_t*>(remote_), remote_size_);  // NOLINT
      remote_ = nullptr;
    }
    if (staging_ != nullptr) {
      ::munmap(staging_, staging_size_);
      staging_ = nullptr;
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

  // Lock order: tx_mu_, then map_mu_; mu_ and dma_mu_ are taken alone.
  std::mutex mu_;  // local segments, directory, importers
  std::uint8_t* local_ = nullptr;
  std::size_t local_size_ = 0;
  std::map<std::uint32_t, Local> segs_;
  std::multimap<std::uint32_t, Local> quarantine_;  // freed, but still imported
  std::map<std::size_t, std::size_t> free_;         // offset -> length
  std::set<Importer> importers_;
  std::uint64_t alloc_no_ = 0;

  std::shared_mutex map_mu_;  // the peer's region mapping
  volatile std::uint8_t* remote_ = nullptr;
  std::size_t remote_size_ = 0;
  std::atomic<long> n_imports_{0};

  std::mutex tx_mu_;  // our notices into the peer's mailbox
  std::uint64_t tx_seq_ = 0;
  std::uint64_t tx_session_ = 0;

  std::mutex dma_mu_;
  std::uint8_t* staging_ = nullptr;
  std::size_t staging_size_ = 0;
  std::size_t staging_pos_ = 0;
  bool has_dma_ = false;

  // reader thread only
  bool ready_ = false;
  std::uint64_t rx_seq_ = 0;
  std::uint32_t rx_gen_ = 0;  // generation of the sender we are reading
};

}  // namespace

std::unique_ptr<backend::MappedBackend> make_ntb_backend() {
  return std::make_unique<NtbBackend>();
}

}  // namespace pushseg::backends
