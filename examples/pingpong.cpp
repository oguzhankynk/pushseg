// Ping-pong between two processes, push-only.
//
// Each side owns an inbox segment and holds a connection to the other side's
// inbox. A side only ever spins on its *own* memory and only ever *writes* to
// the remote one: the pattern that is cheap on every backend.
//
//   pushseg_pingpong <config> <self-node> <peer-node> ping|pong [iterations]
//
// Start "pong" first or second; connect() waits until the peer publishes.

#include "pushseg/pushseg.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

constexpr pushseg::SegNo kInbox = 1;
constexpr std::size_t kBulk = 1U << 20U;  // throughput phase: 1 MiB per message

void spin_until(const pushseg::LocalSegment& inbox, std::uint64_t expected) {
  while (pushseg::load<std::uint64_t>(inbox.data()) != expected) {
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 5) {
    std::cerr << "usage: " << argv[0] << " <config> <self-node> <peer-node> ping|pong [iters]\n";
    return 2;
  }
  try {
    const pushseg::Config cfg = pushseg::Config::load(argv[1]);
    const auto self = static_cast<pushseg::NodeId>(std::strtoul(argv[2], nullptr, 10));
    const auto peer = static_cast<pushseg::NodeId>(std::strtoul(argv[3], nullptr, 10));
    const std::string role = argv[4];
    const std::uint64_t iters = argc > 5 ? std::strtoull(argv[5], nullptr, 10) : 10000;
    const bool ping = role == "ping";

    pushseg::Context ctx(cfg, self, "pingpong");
    pushseg::LocalSegment inbox = ctx.create_segment(kInbox, 64 + kBulk);
    inbox.publish();
    pushseg::Connection out = ctx.connect(peer, "pingpong", 0, kInbox, std::chrono::seconds(30));
    std::cout << "connected to node " << peer << " (" << out.size() << " bytes)\n";

    // Phase 1: latency. Flag at offset 0.
    std::vector<double> rtt_us;
    rtt_us.reserve(iters);
    for (std::uint64_t i = 1; i <= iters; ++i) {
      if (ping) {
        const auto t0 = std::chrono::steady_clock::now();
        if (out.store<std::uint64_t>(0, i) != pushseg::Status::kOk) {
          throw pushseg::Error(pushseg::Status::kDisconnected, "peer gone");
        }
        spin_until(inbox, i);
        rtt_us.push_back(
            std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0)
                .count());
      } else {
        spin_until(inbox, i);
        if (out.store<std::uint64_t>(0, i) != pushseg::Status::kOk) {
          throw pushseg::Error(pushseg::Status::kDisconnected, "peer gone");
        }
      }
    }

    // Phase 2: throughput. ping pushes 1 MiB blocks + flag at offset 8, pong acks.
    constexpr std::uint64_t kBlocks = 256;
    std::vector<std::uint8_t> block(kBulk, 0x5A);
    const auto t0 = std::chrono::steady_clock::now();
    for (std::uint64_t b = 1; b <= kBlocks; ++b) {
      if (ping) {
        (void)out.write(64, block.data(), block.size());
        (void)out.store<std::uint64_t>(8, b);
        while (pushseg::load<std::uint64_t>(inbox.at<std::uint64_t>(8)) != b) {
        }
      } else {
        while (pushseg::load<std::uint64_t>(inbox.at<std::uint64_t>(8)) != b) {
        }
        (void)out.store<std::uint64_t>(8, b);
      }
    }
    const double secs =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    if (ping) {
      std::sort(rtt_us.begin(), rtt_us.end());
      std::cout << "round trip (us): min " << rtt_us.front() << "  p50 "
                << rtt_us[rtt_us.size() / 2] << "  p99 " << rtt_us[rtt_us.size() * 99 / 100]
                << "\n";
      std::cout << "throughput: " << (static_cast<double>(kBlocks * kBulk) / secs / 1e6)
                << " MB/s (1 MiB blocks, acked)\n";
    }
    (void)out.flush();
  } catch (const pushseg::Error& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
