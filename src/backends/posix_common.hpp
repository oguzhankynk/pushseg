#ifndef PUSHSEG_SRC_BACKENDS_POSIX_COMMON_HPP
#define PUSHSEG_SRC_BACKENDS_POSIX_COMMON_HPP

// Helpers shared by the same-host POSIX backends (mq, shm).

#include "pushseg/backend.hpp"

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <string>
#include <sys/file.h>
#include <sys/types.h>

#include <fcntl.h>
#include <unistd.h>

#if defined(__SANITIZE_THREAD__)
#define PUSHSEG_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define PUSHSEG_TSAN 1
#endif
#endif

#ifdef PUSHSEG_TSAN
extern "C" void __tsan_acquire(void* addr);  // NOLINT(bugprone-reserved-identifier)
extern "C" void __tsan_release(void* addr);  // NOLINT(bugprone-reserved-identifier)
#endif

namespace pushseg::backends::posix {

// mq_send() -> mq_receive() orders memory like any syscall pair, but
// ThreadSanitizer does not intercept message queues (it does for sockets), so
// it would report false races between a sender's writes and the receiver's
// reads. Tell it about the edge. One token for all queues: coarser than the
// truth, which can only hide races, never invent them. No-op without TSan.
#ifdef PUSHSEG_TSAN
inline char g_mq_sync_token;
inline void mq_release() {
  __tsan_release(&g_mq_sync_token);
}
inline void mq_acquire() {
  __tsan_acquire(&g_mq_sync_token);
}
#else
inline void mq_release() {}
inline void mq_acquire() {}
#endif

inline std::string opt(const backend::Options& o, const char* key, const char* def) {
  const auto it = o.find(key);
  return it == o.end() ? std::string(def) : it->second;
}

inline long opt_long(const backend::Options& o, const char* key, long def) {
  const auto it = o.find(key);
  return it == o.end() ? def : std::strtol(it->second.c_str(), nullptr, 10);
}

// "<node>.<app>.<inst>": unique for a process on a host.
inline std::string identity(const backend::Endpoint& e) {
  return std::to_string(e.node) + "." + std::to_string(e.app) + "." + std::to_string(e.inst);
}

// Whether a process exists (an unknown pid counts as alive).
inline bool alive(pid_t pid) {
  return pid <= 0 || ::kill(pid, 0) == 0 || errno == EPERM;
}

// Absolute CLOCK_REALTIME deadline `ms` from now, for mq_timed*().
inline timespec deadline_in(int ms) {
  timespec ts{};
  ::clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_nsec += static_cast<long>(ms % 1000) * 1000000L;
  ts.tv_sec += ms / 1000 + ts.tv_nsec / 1000000000L;
  ts.tv_nsec %= 1000000000L;
  return ts;
}

// The pid written into a lock file, or 0.
inline pid_t read_pid(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return 0;
  }
  char buf[32] = {};
  const ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
  ::close(fd);
  return n > 0 ? static_cast<pid_t>(std::strtol(buf, nullptr, 10)) : 0;
}

// An flock'ed file that makes an identity unique on the host while its owner
// lives, and holds the owner's pid. Holding it, the owner may remove anything
// a crashed predecessor with the same identity left behind.
class IdentityLock {
public:
  IdentityLock() = default;
  ~IdentityLock() {
    release();
  }
  IdentityLock(const IdentityLock&) = delete;
  IdentityLock& operator=(const IdentityLock&) = delete;
  IdentityLock(IdentityLock&&) = delete;
  IdentityLock& operator=(IdentityLock&&) = delete;

  // kOk, kExists (another live process holds it) or kIoError.
  Status acquire(const std::string& path) {
    path_ = path;
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (fd_ < 0) {
      return Status::kIoError;
    }
    if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
      const int err = errno;
      ::close(fd_);
      fd_ = -1;
      return err == EWOULDBLOCK ? Status::kExists : Status::kIoError;
    }
    const std::string pid = std::to_string(::getpid()) + "\n";
    if (::ftruncate(fd_, 0) != 0 ||
        ::write(fd_, pid.data(), pid.size()) != static_cast<ssize_t>(pid.size())) {
      release();
      return Status::kIoError;
    }
    return Status::kOk;
  }

  void release() {
    if (fd_ >= 0) {
      ::unlink(path_.c_str());  // while still holding the lock
      ::close(fd_);
      fd_ = -1;
    }
  }

private:
  std::string path_;
  int fd_ = -1;
};

}  // namespace pushseg::backends::posix

#endif  // PUSHSEG_SRC_BACKENDS_POSIX_COMMON_HPP
