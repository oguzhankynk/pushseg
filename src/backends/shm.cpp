// Built-in mapped backend over POSIX shared memory (shm_overview(7)).
// Same host only: every node linked through "shm" must run on one machine.
//
// Writers map the owner's segment and store into it directly: no copy, no
// system call on the data path.
//
// Naming (options from [backend.shm]):
//   segment        <name_prefix>.s.<node>.<key>        one per local segment
//   event queue    <name_prefix>.e.<node>.<app>.<inst> one per process (POSIX mq)
//   lock file      <lock_dir>/<prefix>.shm.<node>.<app>.<inst>.lock
//
// A segment object is [header page][data]. The header carries a magic number,
// the size, the owner's pid and an "exported" flag; writers check the flag and
// the pid before every interrupt, so a destroyed segment or a dead owner turns
// into kDisconnected.
//
// Connect, disconnect and interrupt notifications travel to the owner's event
// queue, read by one thread per process. Queued events are never lost, which
// gives sticky doorbells. A writer that dies without releasing its mapping is
// not reported to the owner.

#include "pushseg/backend.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>

#include <dirent.h>
#include <fcntl.h>
#include <mqueue.h>
#include <poll.h>
#include <unistd.h>

#include "backends/posix_common.hpp"

namespace pushseg::backends {
namespace {

using backend::Endpoint;
using backend::MappedHost;
using backend::Options;
using backend::RemoteSegment;

constexpr std::uint64_t kMagic = 0x4D48535345535550ULL;  // "PUSESSHM"
constexpr std::size_t kHeaderBytes = 4096;               // data starts page aligned
constexpr int kPollMs = 200;
const mqd_t kBadMq = -1;  // mq_open() error value; mqd_t is an fd on Linux

struct Header {
  std::uint64_t magic;
  std::uint64_t size;
  std::int32_t owner_pid;
  std::uint32_t exported;  // accessed atomically
};

enum class EventType : std::uint8_t { kConnect = 1, kDisconnect, kInterrupt };

struct Event {
  EventType type;
  std::uint8_t app;
  std::uint16_t node;
  std::uint32_t key;
  std::uint8_t inst;
  std::uint8_t pad[7];
  std::uint64_t imm;
};
static_assert(sizeof(Event) == 24, "event layout");

std::uint32_t load_exported(const Header* h) {
  return __atomic_load_n(&h->exported, __ATOMIC_ACQUIRE);
}

void store_exported(Header* h, std::uint32_t v) {
  __atomic_store_n(&h->exported, v, __ATOMIC_RELEASE);
}

// A writer's mapping of a remote segment.
struct Import {
  unsigned char* map = nullptr;
  std::size_t len = 0;
  std::uint32_t key = 0;
  mqd_t events = kBadMq;  // the owner's event queue
};

class ShmBackend final : public backend::MappedBackend {
public:
  ~ShmBackend() override {
    shutdown();
    for (auto& kv : segs_) {
      unmap_and_unlink(kv.first, kv.second);
    }
  }

  Status start(const Endpoint& self, const Options& opts, MappedHost& host) override {
    self_ = self;
    host_ = &host;
    prefix_ = posix::opt(opts, "name_prefix", "/pushseg");
    lock_dir_ = posix::opt(opts, "lock_dir", "/dev/shm");
    const long maxmsg = posix::opt_long(opts, "event_maxmsg", 8);
    if (prefix_.size() < 2 || prefix_[0] != '/' || prefix_.find('/', 1) != std::string::npos ||
        maxmsg <= 0) {
      return Status::kInvalidArgument;
    }
    if (const Status st = lock_.acquire(lock_dir_ + "/" + prefix_.substr(1) + ".shm." +
                                        posix::identity(self) + ".lock");
        st != Status::kOk) {
      return st;
    }
    // Holding the lock, whatever a crashed predecessor left is stale.
    remove_stale_segments();
    events_name_ = event_queue_name(self);
    ::mq_unlink(events_name_.c_str());
    mq_attr attr{};
    attr.mq_maxmsg = maxmsg;
    attr.mq_msgsize = static_cast<long>(sizeof(Event));
    events_ = ::mq_open(
        events_name_.c_str(), O_RDONLY | O_CREAT | O_EXCL | O_NONBLOCK | O_CLOEXEC, 0600, &attr);
    wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (events_ == kBadMq || wake_fd_ < 0) {
      shutdown();
      return Status::kIoError;
    }
    reader_ = std::thread([this] { event_loop(); });
    return Status::kOk;
  }

  Status alloc_segment(std::uint32_t key, std::size_t size, void** base) override {
    const std::string name = segment_name(self_.node, key);
    ::shm_unlink(name.c_str());  // our identity's key: anything there is stale
    const int fd = ::shm_open(name.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) {
      return errno == EEXIST ? Status::kExists : Status::kIoError;
    }
    const std::size_t len = kHeaderBytes + size;
    void* p = MAP_FAILED;
    if (::ftruncate(fd, static_cast<off_t>(len)) == 0) {
      p = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    }
    ::close(fd);
    if (p == MAP_FAILED) {
      ::shm_unlink(name.c_str());
      return Status::kIoError;
    }
    auto* map = static_cast<unsigned char*>(p);
    auto* h = reinterpret_cast<Header*>(map);
    h->size = size;
    h->owner_pid = ::getpid();
    store_exported(h, 0);
    __atomic_store_n(&h->magic, kMagic, __ATOMIC_RELEASE);  // last: the header is complete
    std::lock_guard<std::mutex> lk(mu_);
    segs_[key] = Local{map, len};
    *base = map + kHeaderBytes;
    return Status::kOk;
  }

  Status export_segment(std::uint32_t key) override {
    return set_exported(key, 1) ? Status::kOk : Status::kNotFound;
  }

  void unexport_segment(std::uint32_t key) override {
    (void)set_exported(key, 0);
  }

  void free_segment(std::uint32_t key) override {
    Local seg;
    {
      std::lock_guard<std::mutex> lk(mu_);
      const auto it = segs_.find(key);
      if (it == segs_.end()) {
        return;
      }
      seg = it->second;
      segs_.erase(it);
    }
    unmap_and_unlink(key, seg);
  }

  Status import_segment(const Endpoint& remote, std::uint32_t key, RemoteSegment& out) override {
    const int fd = ::shm_open(segment_name(remote.node, key).c_str(), O_RDWR | O_CLOEXEC, 0);
    if (fd < 0) {
      return Status::kNotFound;
    }
    struct stat st {};
    void* p = MAP_FAILED;
    std::size_t len = 0;
    if (::fstat(fd, &st) == 0 && static_cast<std::size_t>(st.st_size) > kHeaderBytes) {
      len = static_cast<std::size_t>(st.st_size);
      p = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    }
    ::close(fd);
    if (p == MAP_FAILED) {
      return Status::kNotFound;  // not created yet, or still being sized
    }
    auto* map = static_cast<unsigned char*>(p);
    const auto* h = reinterpret_cast<const Header*>(map);
    if (__atomic_load_n(&h->magic, __ATOMIC_ACQUIRE) != kMagic || load_exported(h) == 0 ||
        kHeaderBytes + h->size > len || !posix::alive(h->owner_pid)) {
      ::munmap(map, len);
      return Status::kNotFound;
    }
    // Blocking, so that mq_timedsend() waits for room until its deadline.
    const mqd_t events = ::mq_open(event_queue_name(remote).c_str(), O_WRONLY | O_CLOEXEC);
    if (events == kBadMq) {
      ::munmap(map, len);
      return Status::kNotFound;
    }
    auto* imp = new Import{map, len, key, events};
    out.base = map + kHeaderBytes;
    out.size = h->size;
    out.handle = imp;
    (void)send_event(*imp, EventType::kConnect, 0, false);
    return Status::kOk;
  }

  void release_segment(RemoteSegment& seg) override {
    auto* imp = static_cast<Import*>(seg.handle);
    if (imp == nullptr) {
      return;
    }
    (void)send_event(*imp, EventType::kDisconnect, 0, false);
    ::mq_close(imp->events);
    ::munmap(imp->map, imp->len);
    delete imp;
    seg = RemoteSegment{};
  }

  Status interrupt(const RemoteSegment& seg, std::uint64_t imm) override {
    const auto* imp = static_cast<const Import*>(seg.handle);
    return send_event(*imp, EventType::kInterrupt, imm, true);
  }

  void stop() override {
    shutdown();
  }

private:
  struct Local {
    unsigned char* map = nullptr;
    std::size_t len = 0;
  };

  std::string segment_name(NodeId node, std::uint32_t key) const {
    char hex[9];
    std::snprintf(hex, sizeof(hex), "%08x", key);
    return prefix_ + ".s." + std::to_string(node) + "." + hex;
  }

  // Segments of this identity: <prefix>.s.<node>.<key>, the key starting with
  // our app and inst. POSIX shared memory objects appear under /dev/shm on Linux.
  void remove_stale_segments() const {
    char key_prefix[16];
    std::snprintf(key_prefix, sizeof(key_prefix), "%02x%02x", self_.app, self_.inst);
    const std::string mine =
        prefix_.substr(1) + ".s." + std::to_string(self_.node) + "." + key_prefix;
    DIR* dir = ::opendir("/dev/shm");
    if (dir == nullptr) {
      return;
    }
    while (const dirent* e = ::readdir(dir)) {
      const std::string name = e->d_name;
      if (name.size() == mine.size() + 4 && name.compare(0, mine.size(), mine) == 0) {
        ::shm_unlink(("/" + name).c_str());
      }
    }
    ::closedir(dir);
  }

  std::string event_queue_name(const Endpoint& e) const {
    return prefix_ + ".e." + posix::identity(e);
  }

  bool set_exported(std::uint32_t key, std::uint32_t value) {
    std::lock_guard<std::mutex> lk(mu_);
    const auto it = segs_.find(key);
    if (it == segs_.end()) {
      return false;
    }
    store_exported(reinterpret_cast<Header*>(it->second.map), value);
    return true;
  }

  void unmap_and_unlink(std::uint32_t key, const Local& seg) const {
    store_exported(reinterpret_cast<Header*>(seg.map), 0);  // writers see it before unmap
    ::munmap(seg.map, seg.len);
    ::shm_unlink(segment_name(self_.node, key).c_str());
  }

  // `wait`: block while the owner's queue is full (interrupts must not be
  // lost); connect/disconnect notices are best effort.
  Status send_event(const Import& imp, EventType type, std::uint64_t imm, bool wait) const {
    const auto* h = reinterpret_cast<const Header*>(imp.map);
    Event ev{};
    ev.type = type;
    ev.node = self_.node;
    ev.app = self_.app;
    ev.inst = self_.inst;
    ev.key = imp.key;
    ev.imm = imm;
    while (true) {
      if (load_exported(h) == 0 || !posix::alive(h->owner_pid)) {
        return Status::kDisconnected;  // segment destroyed, or owner gone
      }
      const timespec ts = posix::deadline_in(wait ? kPollMs : 0);
      posix::mq_release();
      if (::mq_timedsend(imp.events, reinterpret_cast<const char*>(&ev), sizeof(ev), 0, &ts) == 0) {
        return Status::kOk;
      }
      if (errno == EINTR || (errno == ETIMEDOUT && wait)) {
        continue;
      }
      return wait ? Status::kDisconnected : Status::kOk;
    }
  }

  void event_loop() {
    while (true) {
      pollfd fds[2] = {{events_, POLLIN, 0}, {wake_fd_, POLLIN, 0}};
      if (::poll(fds, 2, -1) < 0 && errno != EINTR) {
        return;
      }
      if (stopping_.load(std::memory_order_acquire)) {
        return;
      }
      Event ev{};
      if (::mq_receive(events_, reinterpret_cast<char*>(&ev), sizeof(ev), nullptr) !=
          static_cast<ssize_t>(sizeof(ev))) {
        continue;
      }
      posix::mq_acquire();
      const PeerId from{ev.node, ev.app, ev.inst};
      switch (ev.type) {
        case EventType::kConnect:
          host_->on_peer_connect(ev.key, from);
          break;
        case EventType::kDisconnect:
          host_->on_peer_disconnect(ev.key, from, false);
          break;
        case EventType::kInterrupt:
          host_->on_interrupt(ev.key, from, ev.imm);
          break;
      }
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
      reader_.join();
    }
    if (events_ != kBadMq) {
      ::mq_close(events_);
      ::mq_unlink(events_name_.c_str());
      events_ = kBadMq;
    }
    if (wake_fd_ >= 0) {
      ::close(wake_fd_);
      wake_fd_ = -1;
    }
    lock_.release();
  }

  Endpoint self_;
  MappedHost* host_ = nullptr;
  std::string prefix_;
  std::string lock_dir_;
  std::string events_name_;
  posix::IdentityLock lock_;
  mqd_t events_ = kBadMq;
  int wake_fd_ = -1;
  std::atomic<bool> stopping_{false};
  std::thread reader_;

  std::mutex mu_;
  std::map<std::uint32_t, Local> segs_;
};

}  // namespace

std::unique_ptr<backend::MappedBackend> make_shm_backend() {
  return std::make_unique<ShmBackend>();
}

}  // namespace pushseg::backends
