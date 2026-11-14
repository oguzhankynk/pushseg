// API-level tests: handles, error paths, config files, backend registration,
// the mapped DMA path, and robustness against a misbehaving peer.

#include "pushseg/backend.hpp"
#include "pushseg/pushseg.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <netinet/in.h>
#include <set>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <vector>

#include <unistd.h>

#include "core/wire.hpp"
#include "test_util.hpp"

namespace pushseg::backends {
// Built-ins, reused here to build test backends on top of them.
std::unique_ptr<backend::StreamBackend> make_tcp_backend();
std::unique_ptr<backend::MappedBackend> make_loopmem_backend();
}  // namespace pushseg::backends

namespace pushseg {
namespace {

using namespace pushseg::test;  // NOLINT(google-build-using-namespace)

// ---------------------------------------------------------------------------
// Status / Error
// ---------------------------------------------------------------------------

TEST(StatusTest, EveryCodeHasADistinctName) {
  std::set<std::string> names;
  for (int i = 0; i <= static_cast<int>(Status::kIoError); ++i) {
    const std::string n = to_string(static_cast<Status>(i));
    EXPECT_NE(n, "unknown");
    names.insert(n);
  }
  EXPECT_EQ(names.size(), static_cast<std::size_t>(Status::kIoError) + 1);
  EXPECT_STREQ(to_string(static_cast<Status>(999)), "unknown");
}

TEST(StatusTest, ErrorCarriesStatusAndMessage) {
  const Error e(Status::kTimeout, "waited too long");
  EXPECT_EQ(e.status(), Status::kTimeout);
  EXPECT_NE(std::string(e.what()).find("timeout"), std::string::npos);
  EXPECT_NE(std::string(e.what()).find("waited too long"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Config files
// ---------------------------------------------------------------------------

TEST(ConfigFileTest, LoadsFromDisk) {
  const std::string path = ::testing::TempDir() + "pushseg_config_test.conf";
  {
    std::ofstream f(path);
    f << "[apps]\nalgo = 3\n[node.5]\ntcp = 10.0.0.5\n[links]\ndefault = shm\n";
  }
  const Config cfg = Config::load(path);
  EXPECT_EQ(cfg.app_id("algo"), 3);
  EXPECT_EQ(cfg.nodes.at(5).at("tcp"), "10.0.0.5");
  EXPECT_EQ(cfg.backend_for(5, 5), "shm");
  std::remove(path.c_str());
}

TEST(ConfigFileTest, MissingFileIsNotFound) {
  EXPECT_EQ(thrown_status([] { (void)Config::load("/nonexistent/pushseg.conf"); }),
            Status::kNotFound);
}

TEST(ConfigFileTest, MoreMalformedInput) {
  for (const char* text : {"[apps\n",                                   // unterminated section
                           "[apps]\n = 1\n",                            // empty key
                           "[links]\ndefault =\n",                      // empty backend name
                           "[apps]\nx = \n",                            // empty number
                           "[apps]\nx = 99999999999999999999999\n"}) {  // overflow
    EXPECT_EQ(thrown_status([&] { (void)Config::parse(text); }), Status::kInvalidArgument) << text;
  }
}

// ---------------------------------------------------------------------------
// Empty and moved-from handles
// ---------------------------------------------------------------------------

TEST(HandleTest, EmptySegmentIsInert) {
  LocalSegment s;
  EXPECT_FALSE(s.valid());
  EXPECT_EQ(s.data(), nullptr);
  EXPECT_EQ(s.size(), 0U);
  EXPECT_EQ(s.key().raw(), 0U);
  Event ev;
  EXPECT_EQ(s.wait_event(ev, 1ms), Status::kClosed);
  EXPECT_FALSE(s.poll_event(ev));
  EXPECT_EQ(thrown_status([&] { s.publish(); }), Status::kClosed);
  s.unpublish();
  s.set_event_handler([](const Event&) {});
  s.destroy();
}

TEST(HandleTest, EmptyConnectionIsInert) {
  Connection c;
  EXPECT_FALSE(c.valid());
  EXPECT_EQ(c.size(), 0U);
  const char b = 0;
  EXPECT_EQ(c.write(0, &b, 1), Status::kClosed);
  EXPECT_EQ(c.store<std::uint8_t>(0, 1), Status::kClosed);
  EXPECT_EQ(c.write_notify(0, &b, 1, 1), Status::kClosed);
  EXPECT_EQ(c.notify(1), Status::kClosed);
  EXPECT_EQ(c.flush(), Status::kClosed);
  c.disconnect();
}

TEST(HandleTest, MovedFromContextIsInert) {
  const Config cfg = make_config("tcp");
  Context a(cfg, 0, "test");
  Context b(std::move(a));
  EXPECT_EQ(b.self(), (PeerId{0, 1, 0}));
  EXPECT_EQ(a.self(), PeerId{});  // NOLINT(bugprone-use-after-move): testing it
  EXPECT_EQ(thrown_status([&] { (void)a.create_segment(kSeg, 64); }), Status::kClosed);
  EXPECT_EQ(thrown_status([&] { (void)a.connect(1, kKey, 1ms); }), Status::kClosed);
  EXPECT_EQ(thrown_status([&] { (void)a.connect(1, "test", 0, kSeg, 1ms); }), Status::kClosed);
}

TEST(HandleTest, MoveAssignReleasesThePreviousObject) {
  const Config cfg = make_config("tcp");
  Context owner(cfg, 0, "test");
  Context writer(cfg, 1, "test");

  LocalSegment s1 = owner.create_segment(1, 64);
  LocalSegment s2 = owner.create_segment(2, 64);
  s1 = std::move(s2);  // segment 1 is destroyed
  EXPECT_EQ(s1.key().seg, 2);
  EXPECT_EQ(thrown_status([&] { (void)owner.create_segment(1, 64); }), Status::kOk);
  s1.publish();

  Connection c1 = writer.connect(0, SegKey{1, 0, 2}, 2s);
  Connection c2 = writer.connect(0, SegKey{1, 0, 2}, 2s);
  c1 = std::move(c2);  // the first connection disconnects
  EXPECT_EQ(c1.notify(5), Status::kOk);
  // Events of different connections are unordered: collect until all arrived.
  int connects = 0;
  int disconnects = 0;
  int notifies = 0;
  Event ev;
  while ((connects < 2 || disconnects < 1 || notifies < 1) &&
         s1.wait_event(ev, 2s) == Status::kOk) {
    connects += ev.type == EventType::kConnect ? 1 : 0;
    disconnects += ev.type == EventType::kDisconnect ? 1 : 0;
    notifies += ev.type == EventType::kNotify ? 1 : 0;
  }
  EXPECT_EQ(connects, 2);
  EXPECT_EQ(disconnects, 1);
  EXPECT_EQ(notifies, 1);

  // Assigning a new context shuts the old one down: its segments close.
  owner = Context(cfg, 2, "test");
  Status st = Status::kOk;
  while (st == Status::kOk) {  // drain anything still queued
    st = s1.wait_event(ev, 1s);
  }
  EXPECT_EQ(st, Status::kClosed);
  EXPECT_EQ(thrown_status([&] { s1.publish(); }), Status::kClosed);
}

// ---------------------------------------------------------------------------
// Context errors and lifecycle
// ---------------------------------------------------------------------------

TEST(ContextErrorTest, BadArguments) {
  const Config cfg = make_config("tcp");
  EXPECT_EQ(thrown_status([&] { Context c(cfg, 9, "test"); }), Status::kInvalidArgument);
  EXPECT_EQ(thrown_status([&] { Context c(cfg, 0, "nope"); }), Status::kNotFound);
  Context ctx(cfg, 0, "test");
  EXPECT_EQ(thrown_status([&] { (void)ctx.create_segment(kSeg, 0); }), Status::kInvalidArgument);
  EXPECT_EQ(thrown_status([&] { (void)ctx.connect(9, kKey, 1ms); }), Status::kInvalidArgument);
}

TEST(ContextErrorTest, TcpRejectsBadAddressAndInstance) {
  Config cfg = make_config("tcp");
  EXPECT_EQ(thrown_status([&] { Context c(cfg, 0, "test", 40); }), Status::kInvalidArgument);
  cfg.nodes[0]["tcp"] = "127.0.0.1:notaport";
  EXPECT_EQ(thrown_status([&] { Context c(cfg, 0, "test"); }), Status::kInvalidArgument);
}

TEST(ContextErrorTest, ShmRejectsBadPrefix) {
  Config cfg = make_config("shm");
  cfg.backend_options["shm"]["name_prefix"] = "no-leading-slash";
  EXPECT_EQ(thrown_status([&] { Context c(cfg, 0, "test"); }), Status::kInvalidArgument);
}

TEST(ContextErrorTest, AtMostOneMappedBackendPerNode) {
  backend::register_mapped_backend("loopmem2", [] { return backends::make_loopmem_backend(); });
  Config cfg = make_config("tcp");
  cfg.set_link(0, 1, "loopmem");
  cfg.set_link(0, 2, "loopmem2");
  EXPECT_EQ(thrown_status([&] { Context c(cfg, 0, "test"); }), Status::kInvalidArgument);
}

TEST(LifecycleTest, UnpublishStopsNewWritersAndPollWorks) {
  const Config cfg = make_config("tcp");
  Context owner(cfg, 0, "test");
  Context writer(cfg, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  Connection c = writer.connect(0, "test", 0, kSeg, 2s);  // by application name
  seg.unpublish();
  EXPECT_EQ(thrown_status([&] { (void)writer.connect(0, kKey, 200ms); }), Status::kTimeout);
  EXPECT_EQ(c.notify(3), Status::kOk);  // existing writers keep working

  Event ev;
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  bool got_notify = false;
  while (!got_notify && std::chrono::steady_clock::now() < deadline) {
    while (seg.poll_event(ev)) {
      got_notify = got_notify || (ev.type == EventType::kNotify && ev.imm == 3);
    }
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(got_notify);
  seg.set_event_handler(nullptr);  // back to queueing
}

// ---------------------------------------------------------------------------
// User-registered backends
// ---------------------------------------------------------------------------

TEST(RegistrationTest, CustomStreamBackendIsUsed) {
  static std::atomic<int> created{0};
  backend::register_stream_backend("mytcp", [] {
    ++created;
    return backends::make_tcp_backend();
  });
  const int before = created.load();
  Config cfg = make_config("mytcp");
  for (auto& n : cfg.nodes) {
    n.second["mytcp"] = n.second["tcp"];
  }
  Context owner(cfg, 0, "test");
  Context writer(cfg, 1, "test");
  EXPECT_EQ(created.load() - before, 2);
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);
  ASSERT_EQ(c.store<std::uint32_t>(4, 77), Status::kOk);
  ASSERT_EQ(c.flush(), Status::kOk);
  EXPECT_EQ(load<std::uint32_t>(seg.at<std::uint32_t>(4)), 77U);
}

// A mapped backend with a (simulated) DMA engine, built on loopmem: what a
// user's PCIe backend looks like. DMA copies are deferred until dma_wait(), so
// a missing wait in the core shows up as missing data.
class FakeDmaBackend final : public backend::MappedBackend {
public:
  static inline std::atomic<int> dma_writes{0};
  static inline std::atomic<int> dma_waits{0};

  Status start(const backend::Endpoint& self,
               const backend::Options& opts,
               backend::MappedHost& host) override {
    return inner_->start(self, opts, host);
  }
  Status alloc_segment(std::uint32_t key, std::size_t size, void** base) override {
    return inner_->alloc_segment(key, size, base);
  }
  Status export_segment(std::uint32_t key) override {
    return inner_->export_segment(key);
  }
  void unexport_segment(std::uint32_t key) override {
    inner_->unexport_segment(key);
  }
  void free_segment(std::uint32_t key) override {
    inner_->free_segment(key);
  }
  Status import_segment(const backend::Endpoint& remote,
                        std::uint32_t key,
                        backend::RemoteSegment& out) override {
    return inner_->import_segment(remote, key, out);
  }
  void release_segment(backend::RemoteSegment& seg) override {
    inner_->release_segment(seg);
  }
  Status interrupt(const backend::RemoteSegment& seg, std::uint64_t imm) override {
    return inner_->interrupt(seg, imm);
  }
  void stop() override {
    inner_->stop();
  }

  bool has_dma() const override {
    return true;
  }
  Status dma_write(const backend::RemoteSegment& seg,
                   std::uint64_t off,
                   const void* src,
                   std::size_t len) override {
    ++dma_writes;
    const auto* p = static_cast<const std::uint8_t*>(src);
    pending_.push_back({seg.base + off, std::vector<std::uint8_t>(p, p + len)});
    return Status::kOk;
  }
  Status dma_wait(const backend::RemoteSegment& /*seg*/) override {
    ++dma_waits;
    for (auto& j : pending_) {
      std::memcpy(const_cast<std::uint8_t*>(j.dst), j.data.data(), j.data.size());
    }
    pending_.clear();
    return Status::kOk;
  }

private:
  struct Job {
    volatile std::uint8_t* dst;
    std::vector<std::uint8_t> data;
  };
  std::unique_ptr<backend::MappedBackend> inner_ = backends::make_loopmem_backend();
  std::vector<Job> pending_;
};

TEST(RegistrationTest, MappedBackendDmaPathKeepsTheContract) {
  backend::register_mapped_backend("fakedma", [] { return std::make_unique<FakeDmaBackend>(); });
  Config cfg = make_config("fakedma");
  cfg.backend_options["fakedma"] = {{"dma_threshold", "1024"}};
  Context owner(cfg, 0, "test");
  Context writer(cfg, 1, "test");
  LocalSegment seg = owner.create_segment(kSeg, 8192);
  seg.publish();
  Connection c = writer.connect(0, kKey, 2s);

  const std::vector<std::uint8_t> big(4096, 0xAB);
  const std::vector<std::uint8_t> small(100, 0xCD);
  // Layout: [0,100) small, [256,4352) big, 4352 flag, [5000,5100) small.
  constexpr std::uint64_t kBigOff = 256;
  constexpr std::uint64_t kFlagOff = kBigOff + 4096;
  const int writes_before = FakeDmaBackend::dma_writes.load();
  ASSERT_EQ(c.write(kBigOff, big.data(), big.size()), Status::kOk);  // >= threshold: DMA
  ASSERT_EQ(c.write(0, small.data(), small.size()), Status::kOk);    // below: memcpy
  ASSERT_EQ(c.store<std::uint64_t>(kFlagOff, 1), Status::kOk);
  ASSERT_EQ(c.write_notify(5000, small.data(), small.size(), 9), Status::kOk);
  ASSERT_EQ(c.flush(), Status::kOk);

  EXPECT_EQ(FakeDmaBackend::dma_writes.load() - writes_before, 1);
  EXPECT_GT(FakeDmaBackend::dma_waits.load(), 0);
  EXPECT_EQ(std::memcmp(seg.at<std::uint8_t>(kBigOff), big.data(), big.size()), 0);
  EXPECT_EQ(std::memcmp(seg.data(), small.data(), small.size()), 0);
  EXPECT_EQ(std::memcmp(seg.at<std::uint8_t>(5000), small.data(), small.size()), 0);
  EXPECT_EQ(load<std::uint64_t>(seg.at<std::uint64_t>(kFlagOff)), 1U);
}

// ---------------------------------------------------------------------------
// A misbehaving peer must not take the owner down
// ---------------------------------------------------------------------------

int raw_connect(unsigned port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(static_cast<std::uint16_t>(port));
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&a), sizeof(a)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

// Sends bytes and waits until the owner closes the link.
bool owner_drops_link(unsigned port, const void* data, std::size_t len) {
  const int fd = raw_connect(port);
  if (fd < 0) {
    return false;
  }
  (void)!::send(fd, data, len, MSG_NOSIGNAL);
  timeval tv{2, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  char buf[256];
  ssize_t n = 0;
  do {
    n = ::recv(fd, buf, sizeof(buf), 0);
  } while (n > 0);
  ::close(fd);
  return n == 0;
}

TEST(RobustnessTest, OwnerSurvivesGarbageAndProtocolViolations) {
  const Config cfg = make_config("tcp");
  Context owner(cfg, 0, "test");
  LocalSegment seg = owner.create_segment(kSeg, 64);
  seg.publish();
  const unsigned port = tcp_port(0);

  const std::vector<char> junk(100, 'x');
  EXPECT_TRUE(owner_drops_link(port, junk.data(), junk.size()));

  detail::FrameHeader h;  // data before any connect
  h.op = detail::Op::kNotify;
  EXPECT_TRUE(owner_drops_link(port, &h, sizeof(h)));

  detail::FrameHeader frames[2];  // connect, then a write past the end
  frames[0].op = detail::Op::kConnect;
  frames[0].aux = kKey.raw();
  frames[1].op = detail::Op::kStore;
  frames[1].off = 64;
  frames[1].aux = 8;
  EXPECT_TRUE(owner_drops_link(port, frames, sizeof(frames)));

  // A well-behaved writer is still served.
  Context writer(cfg, 1, "test");
  Connection c = writer.connect(0, kKey, 2s);
  ASSERT_EQ(c.store<std::uint64_t>(8, 42), Status::kOk);
  ASSERT_EQ(c.flush(), Status::kOk);
  EXPECT_EQ(load<std::uint64_t>(seg.at<std::uint64_t>(8)), 42U);
}

}  // namespace
}  // namespace pushseg
