#ifndef PUSHSEG_TESTS_UNIT_TEST_UTIL_HPP
#define PUSHSEG_TESTS_UNIT_TEST_UTIL_HPP

// Helpers shared by the end-to-end tests. All "nodes" are Contexts in the
// test process, so names and ports are made unique per process: ctest runs
// test cases as parallel processes.

#include "pushseg/pushseg.hpp"

#include <chrono>
#include <functional>
#include <string>

#include <unistd.h>

namespace pushseg::test {

using namespace std::chrono_literals;

constexpr SegNo kSeg = 7;
const SegKey kKey{1, 0, kSeg};  // app "test" = 1, inst 0

// TCP base port of a node: distinct per test process, below the ephemeral range.
inline unsigned tcp_base(NodeId node) {
  return 10000U + static_cast<unsigned>(::getpid() % 5000) * 4U + node;
}

// TCP port the process (node, app 1, inst) listens on (tcp backend: base + app*32 + inst).
inline unsigned tcp_port(NodeId node, InstId inst = 0) {
  return tcp_base(node) + 32U + inst;
}

inline Config make_config(const std::string& default_backend) {
  Config cfg;
  cfg.apps["test"] = 1;
  for (NodeId n = 0; n < 3; ++n) {
    cfg.nodes[n]["tcp"] = "127.0.0.1:" + std::to_string(tcp_base(n));
    cfg.nodes[n]["loopmem"] = "";
    cfg.nodes[n]["mq"] = "";
    cfg.nodes[n]["shm"] = "";
  }
  // Per-process names so parallel test processes never share queues, and
  // small queues to stay under RLIMIT_MSGQUEUE with many tests in parallel.
  cfg.backend_options["mq"] = {{"name_prefix", "/pushseg-test-" + std::to_string(::getpid())},
                               {"msgsize", "2048"},
                               {"maxmsg", "4"}};
  cfg.backend_options["shm"] = {{"name_prefix", "/pushseg-test-" + std::to_string(::getpid())}};
  cfg.default_backend = default_backend;
  return cfg;
}

// Status of the pushseg::Error thrown by fn, or kOk if it did not throw.
inline Status thrown_status(const std::function<void()>& fn) {
  try {
    fn();
  } catch (const Error& e) {
    return e.status();
  }
  return Status::kOk;
}

}  // namespace pushseg::test

#endif  // PUSHSEG_TESTS_UNIT_TEST_UTIL_HPP
