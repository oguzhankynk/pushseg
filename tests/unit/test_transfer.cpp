// End-to-end tests of the memory contract, run once per backend:
//   "tcp"      stream level (core wire protocol over TCP on localhost)
//   "shm"      mapped level over POSIX shared memory
//   "loopmem"  mapped level (in-process reference backend)
// All "nodes" live in this one process as separate Contexts.

#include "pushseg/pushseg.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <mutex>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include <unistd.h>

#include "test_util.hpp"

namespace pushseg {
namespace {

using namespace pushseg::test;  // NOLINT(google-build-using-namespace)

class TransferTest : public ::testing::TestWithParam<std::string> {
protected:
  Config cfg_ = make_config(GetParam());
};

TEST_P(TransferTest, WriteNotifyDeliversDataAndEvents) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 4096);
  seg.publish();

  Connection c = writer.connect(0, kKey, 2s);
  EXPECT_EQ(c.size(), 4096U);

  const char msg[] = "hello segment";
  ASSERT_EQ(c.write_notify(100, msg, sizeof(msg), 42), Status::kOk);

  Event ev;
  ASSERT_EQ(seg.wait_event(ev, 2s), Status::kOk);
  EXPECT_EQ(ev.type, EventType::kConnect);
  EXPECT_EQ(ev.peer, (PeerId{1, 1, 0}));
  ASSERT_EQ(seg.wait_event(ev, 2s), Status::kOk);
  EXPECT_EQ(ev.type, EventType::kNotify);
  EXPECT_EQ(ev.imm, 42U);
  EXPECT_STREQ(seg.at<char>(100), msg);
}

TEST_P(TransferTest, ConnectWaitsUntilPublished) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  std::thread later([&] {
    std::this_thread::sleep_for(200ms);
    seg.publish();
  });
  Connection c;
  EXPECT_NO_THROW(c = writer.connect(0, kKey, 5s));
  later.join();
  EXPECT_TRUE(c.valid());
}

TEST_P(TransferTest, ConnectTimesOutWhenNotPublished) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);  // never published
  EXPECT_EQ(thrown_status([&] { (void)writer.connect(0, kKey, 200ms); }), Status::kTimeout);
}

TEST_P(TransferTest, DuplicateSegmentIsRejected) {
  Context owner(cfg_, 0, "test");
  LocalSegment a = owner.create_segment(kSeg, 64);
  EXPECT_EQ(thrown_status([&] { (void)owner.create_segment(kSeg, 64); }), Status::kExists);
  a.destroy();
  EXPECT_EQ(thrown_status([&] { (void)owner.create_segment(kSeg, 64); }), Status::kOk);
}

TEST_P(TransferTest, ChecksRangeAndAlignment) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);
  const std::uint8_t buf[16] = {};
  EXPECT_EQ(c.write(60, buf, 8), Status::kOutOfRange);
  EXPECT_EQ(c.write(~std::uint64_t{0}, buf, 8), Status::kOutOfRange);
  EXPECT_EQ(c.store<std::uint64_t>(4, 1), Status::kMisaligned);
  EXPECT_EQ(c.store<std::uint64_t>(64, 1), Status::kOutOfRange);
  EXPECT_EQ(c.store<std::uint32_t>(60, 1), Status::kOk);
  EXPECT_EQ(c.write(48, buf, 16), Status::kOk);
}

// Contract 3 + 5: stores are single-copy atomic and arrive in order.
TEST_P(TransferTest, StoresNeverTearAndStayOrdered) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 4096);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);

  constexpr std::uint64_t kN = 20000;
  std::atomic<bool> done{false};
  std::uint64_t torn = 0;
  std::uint64_t backwards = 0;
  std::thread reader([&] {
    std::uint64_t last = 0;
    // One more pass after `done`, so a fast writer can never finish unobserved.
    for (bool fin = false; !fin;) {
      fin = done.load();
      const auto v = load<std::uint64_t>(seg.at<std::uint64_t>(64));
      if ((v >> 32U) != (v & 0xFFFFFFFFU)) {
        ++torn;
      }
      if (v < last) {
        ++backwards;
      }
      last = v;
    }
  });
  for (std::uint64_t i = 1; i <= kN; ++i) {
    ASSERT_EQ(c.store<std::uint64_t>(64, (i << 32U) | i), Status::kOk);
  }
  ASSERT_EQ(c.flush(), Status::kOk);
  done = true;
  reader.join();
  EXPECT_EQ(torn, 0U);
  EXPECT_EQ(backwards, 0U);
  EXPECT_EQ(load<std::uint64_t>(seg.at<std::uint64_t>(64)), (kN << 32U) | kN);
}

// Contract 4 + 6: a flag stored after a bulk write guards that data.
// Every round writes a fresh slot, so the reader never looks at bytes that are
// still being written (that would be outside the contract).
TEST_P(TransferTest, FlagGuardsPrecedingBulkWrite) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  constexpr std::size_t kWords = 128;
  constexpr std::size_t kSlot = kWords * 8;
  constexpr std::uint64_t kN = 2000;
  LocalSegment seg = owner.create_segment(kSeg, 64 + kN * kSlot);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);

  std::atomic<bool> done{false};
  std::uint64_t bad = 0;
  std::uint64_t checked = 0;
  std::thread reader([&] {
    std::uint64_t last = 0;
    // One more pass after `done`, so a fast writer can never finish unobserved.
    for (bool fin = false; !fin;) {
      fin = done.load();
      const auto f = load<std::uint64_t>(seg.at<std::uint64_t>(0));
      if (f == 0 || f == last) {
        continue;
      }
      last = f;
      const auto* slot = seg.at<std::uint64_t>(64 + (f - 1) * kSlot);
      for (std::size_t k = 0; k < kWords; ++k) {
        if (slot[k] != f) {  // plain read: the flag's acquire covers it
          ++bad;
        }
      }
      ++checked;
    }
  });
  std::vector<std::uint64_t> block(kWords);
  for (std::uint64_t i = 1; i <= kN; ++i) {
    std::fill(block.begin(), block.end(), i);
    ASSERT_EQ(c.write(64 + (i - 1) * kSlot, block.data(), kSlot), Status::kOk);
    ASSERT_EQ(c.store<std::uint64_t>(0, i), Status::kOk);
  }
  ASSERT_EQ(c.flush(), Status::kOk);
  done = true;
  reader.join();
  EXPECT_EQ(bad, 0U);
  EXPECT_GT(checked, 0U);
}

TEST_P(TransferTest, LargeWriteIsDeliveredWhole) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  constexpr std::size_t kSize = 24U << 20U;  // > one wire frame (16 MiB)
  LocalSegment seg = owner.create_segment(kSeg, kSize);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);

  std::vector<std::uint8_t> src(kSize);
  for (std::size_t i = 0; i < kSize; ++i) {
    src[i] = static_cast<std::uint8_t>(i * 31U + 7U);
  }
  ASSERT_EQ(c.write_notify(0, src.data(), kSize, 1), Status::kOk);
  Event ev;
  do {
    ASSERT_EQ(seg.wait_event(ev, 5s), Status::kOk);
  } while (ev.type != EventType::kNotify);
  EXPECT_EQ(std::memcmp(seg.data(), src.data(), kSize), 0);
}

TEST_P(TransferTest, FlushMakesWritesVisible) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 1U << 20U);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);
  std::vector<std::uint8_t> src(1U << 20U, 0xA5);
  ASSERT_EQ(c.write(0, src.data(), src.size()), Status::kOk);
  ASSERT_EQ(c.flush(), Status::kOk);
  EXPECT_EQ(std::memcmp(seg.data(), src.data(), src.size()), 0);
}

TEST_P(TransferTest, DisconnectIsReportedToOwner) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);
  c.disconnect();
  EXPECT_EQ(c.notify(1), Status::kClosed);

  Event ev;
  ASSERT_EQ(seg.wait_event(ev, 2s), Status::kOk);
  EXPECT_EQ(ev.type, EventType::kConnect);
  ASSERT_EQ(seg.wait_event(ev, 2s), Status::kOk);
  EXPECT_EQ(ev.type, EventType::kDisconnect);
  EXPECT_EQ(ev.imm, 0U);
}

TEST_P(TransferTest, WriterNoticesDestroyedSegment) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);
  seg.destroy();
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  Status st = Status::kOk;
  while (st == Status::kOk && std::chrono::steady_clock::now() < deadline) {
    st = c.notify(1);
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_EQ(st, Status::kDisconnected);
}

TEST_P(TransferTest, MultipleWritersUseOwnSlots) {
  Context owner(cfg_, 0, "test");
  Context w1(cfg_, 1, "test");
  Context w2(cfg_, 2, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  Connection c1 = w1.connect(0, kKey, 2s);
  Connection c2 = w2.connect(0, kKey, 2s);
  ASSERT_EQ(c1.store<std::uint64_t>(8, 111), Status::kOk);
  ASSERT_EQ(c2.store<std::uint64_t>(16, 222), Status::kOk);
  ASSERT_EQ(c1.flush(), Status::kOk);
  ASSERT_EQ(c2.flush(), Status::kOk);
  EXPECT_EQ(load<std::uint64_t>(seg.at<std::uint64_t>(8)), 111U);
  EXPECT_EQ(load<std::uint64_t>(seg.at<std::uint64_t>(16)), 222U);
}

TEST_P(TransferTest, SameNodeDifferentInstances) {
  Context owner(cfg_, 0, "test", 0);
  Context writer(cfg_, 0, "test", 1);
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);
  ASSERT_EQ(c.store<std::uint32_t>(0, 0xC0FFEE), Status::kOk);
  ASSERT_EQ(c.flush(), Status::kOk);
  EXPECT_EQ(load<std::uint32_t>(seg.data()), 0xC0FFEEU);
}

TEST_P(TransferTest, EventHandlerReceivesNotifies) {
  Context owner(cfg_, 0, "test");
  Context writer(cfg_, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  std::mutex mu;
  std::condition_variable cv;
  std::vector<std::uint64_t> got;
  seg.set_event_handler([&](const Event& ev) {
    if (ev.type == EventType::kNotify) {
      std::lock_guard<std::mutex> lk(mu);
      got.push_back(ev.imm);
      cv.notify_all();
    }
  });
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);
  for (std::uint64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(c.notify(i), Status::kOk);
  }
  std::unique_lock<std::mutex> lk(mu);
  ASSERT_TRUE(cv.wait_for(lk, 2s, [&] { return got.size() == 10; }));
  for (std::uint64_t i = 0; i < 10; ++i) {
    EXPECT_EQ(got[i], i);
  }
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         TransferTest,
                         ::testing::Values("tcp", "shm", "loopmem"),
                         [](const ::testing::TestParamInfo<std::string>& i) { return i.param; });

// The owner process dies without any goodbye (SIGKILL). The writer must
// notice: TCP through the closed socket, shm through the owner's pid.
class PeerCrashTest : public ::testing::TestWithParam<std::string> {};

TEST_P(PeerCrashTest, WriterDetectsKilledOwner) {
  const Config cfg = make_config(GetParam());
  int ready[2];
  ASSERT_EQ(::pipe(ready), 0);
  const pid_t child = ::fork();  // no Context exists yet, so we are single-threaded
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::close(ready[0]);
    try {
      Context owner(cfg, 0, "test");
      LocalSegment seg = owner.create_segment(kSeg, 64);
      seg.publish();
      const char ok = 1;
      (void)!::write(ready[1], &ok, 1);
      ::pause();  // until killed
    } catch (...) {
    }
    ::_exit(1);
  }
  ::close(ready[1]);
  char ok = 0;
  ASSERT_EQ(::read(ready[0], &ok, 1), 1);
  ::close(ready[0]);

  Context writer(cfg, 1, "test");
  Connection c = writer.connect(0, kKey, 5s);
  ASSERT_EQ(c.store<std::uint64_t>(0, 1), Status::kOk);
  ASSERT_EQ(c.flush(), Status::kOk);

  ::kill(child, SIGKILL);
  ::waitpid(child, nullptr, 0);

  const auto deadline = std::chrono::steady_clock::now() + 3s;
  Status st = Status::kOk;
  while (st == Status::kOk && std::chrono::steady_clock::now() < deadline) {
    st = c.notify(1);
    std::this_thread::sleep_for(10ms);
  }
  EXPECT_EQ(st, Status::kDisconnected);

  // A restarted owner takes over the dead one's identity (shm: stale lock file,
  // event queue and segments are reclaimed), and cleans up on exit.
  EXPECT_EQ(thrown_status([&] { Context again(cfg, 0, "test"); }), Status::kOk);
}

INSTANTIATE_TEST_SUITE_P(SameHostBackends,
                         PeerCrashTest,
                         ::testing::Values("tcp", "shm"),
                         [](const ::testing::TestParamInfo<std::string>& i) { return i.param; });

// One segment, written over two different backends at once:
// node 1 -> node 0 via the mapped backend, node 2 -> node 0 via TCP.
TEST(MixedBackendTest, OneSegmentTwoTransports) {
  Config cfg = make_config("tcp");
  cfg.set_link(0, 1, "loopmem");
  Context owner(cfg, 0, "test");
  Context mapped_writer(cfg, 1, "test");
  Context tcp_writer(cfg, 2, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  Connection cm = mapped_writer.connect(0, kKey, 2s);
  Connection ct = tcp_writer.connect(0, kKey, 2s);
  ASSERT_EQ(cm.store<std::uint64_t>(0, 1), Status::kOk);
  ASSERT_EQ(ct.store<std::uint64_t>(8, 2), Status::kOk);
  ASSERT_EQ(cm.flush(), Status::kOk);
  ASSERT_EQ(ct.flush(), Status::kOk);
  EXPECT_EQ(load<std::uint64_t>(seg.at<std::uint64_t>(0)), 1U);
  EXPECT_EQ(load<std::uint64_t>(seg.at<std::uint64_t>(8)), 2U);
}

TEST(ContextTest, SameIdentityTwiceFails) {
  for (const char* be : {"tcp", "shm"}) {
    const Config cfg = make_config(be);
    Context a(cfg, 0, "test");
    EXPECT_EQ(thrown_status([&] { Context b(cfg, 0, "test"); }), Status::kExists) << be;
  }
}

TEST(ContextTest, UnknownBackendIsRejected) {
  Config cfg = make_config("nope");
  EXPECT_EQ(thrown_status([&] { Context a(cfg, 0, "test"); }), Status::kInvalidArgument);
}

}  // namespace
}  // namespace pushseg
