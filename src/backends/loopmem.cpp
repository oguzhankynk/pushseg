// Built-in mapped backend "loopmem": every Context in the *same process*
// shares one registry of segments, so an "import" is just a pointer to the
// owner's memory and an "interrupt" is a direct call into the owner's core.
//
// It exists to (1) test the core's mapped path without hardware and (2) serve
// as the reference for writing a real mapped backend (PCIe etc.). The mapping
// to a real API is one-to-one:
//
//   alloc_segment    -> allocate DMA-able memory / create local segment
//   export_segment   -> make it visible to the fabric
//   import_segment   -> connect + map the remote segment (return kNotFound to retry)
//   store_barrier    -> the API's store barrier (default: sfence)
//   interrupt        -> trigger the remote interrupt / doorbell
//
// Interrupts are delivered synchronously on the writer's thread.

#include "pushseg/backend.hpp"

#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <utility>
#include <vector>

namespace pushseg::backends {
namespace {

using backend::Endpoint;
using backend::MappedHost;
using backend::Options;
using backend::RemoteSegment;

struct Segment {
  std::shared_ptr<void> mem;  // stays alive while imported
  std::size_t size = 0;
  bool exported = false;
  const void* owner = nullptr;   // backend instance that allocated it (immutable)
  std::recursive_mutex host_mu;  // guards host against a concurrent stop()
  MappedHost* host = nullptr;
};

struct Import {
  std::shared_ptr<Segment> seg;
  std::uint32_t key = 0;
  PeerId importer;
};

struct Registry {
  std::mutex mu;
  std::map<std::pair<NodeId, std::uint32_t>, std::shared_ptr<Segment>> segs;
};

Registry& registry() {
  static Registry* const r = new Registry();
  return *r;
}

class LoopMemBackend final : public backend::MappedBackend {
public:
  ~LoopMemBackend() override {
    detach_host();
  }

  Status start(const Endpoint& self, const Options& /*opts*/, MappedHost& host) override {
    self_ = PeerId{self.node, self.app, self.inst};
    host_ = &host;
    return Status::kOk;
  }

  Status alloc_segment(std::uint32_t key, std::size_t size, void** base) override {
    constexpr std::size_t kPage = 4096;
    const std::size_t rounded = (size + kPage - 1) / kPage * kPage;
    void* p = std::aligned_alloc(kPage, rounded);
    if (p == nullptr) {
      return Status::kIoError;
    }
    std::memset(p, 0, rounded);
    auto seg = std::make_shared<Segment>();
    seg->mem = std::shared_ptr<void>(p, std::free);
    seg->size = size;
    seg->host = host_;
    seg->owner = this;
    auto& r = registry();
    std::lock_guard<std::mutex> lk(r.mu);
    if (!r.segs.emplace(std::make_pair(self_.node, key), seg).second) {
      return Status::kExists;
    }
    *base = p;
    return Status::kOk;
  }

  Status export_segment(std::uint32_t key) override {
    auto seg = find(self_.node, key);
    if (!seg) {
      return Status::kNotFound;
    }
    std::lock_guard<std::mutex> lk(registry().mu);
    seg->exported = true;
    return Status::kOk;
  }

  void unexport_segment(std::uint32_t key) override {
    if (auto seg = find(self_.node, key)) {
      std::lock_guard<std::mutex> lk(registry().mu);
      seg->exported = false;
    }
  }

  void free_segment(std::uint32_t key) override {
    std::shared_ptr<Segment> seg;
    {
      auto& r = registry();
      std::lock_guard<std::mutex> lk(r.mu);
      const auto it = r.segs.find({self_.node, key});
      if (it == r.segs.end()) {
        return;
      }
      seg = std::move(it->second);
      r.segs.erase(it);
    }
    std::lock_guard<std::recursive_mutex> hl(seg->host_mu);
    seg->host = nullptr;
  }

  Status import_segment(const Endpoint& remote, std::uint32_t key, RemoteSegment& out) override {
    std::shared_ptr<Segment> seg;
    {
      auto& r = registry();
      std::lock_guard<std::mutex> lk(r.mu);
      const auto it = r.segs.find({remote.node, key});
      if (it == r.segs.end() || !it->second->exported) {
        return Status::kNotFound;
      }
      seg = it->second;
    }
    auto* imp = new Import{seg, key, self_};
    out.base = static_cast<volatile std::uint8_t*>(seg->mem.get());
    out.size = seg->size;
    out.handle = imp;
    std::lock_guard<std::recursive_mutex> hl(seg->host_mu);
    if (seg->host != nullptr) {
      seg->host->on_peer_connect(key, self_);
    }
    return Status::kOk;
  }

  void release_segment(RemoteSegment& rs) override {
    auto* imp = static_cast<Import*>(rs.handle);
    if (imp == nullptr) {
      return;
    }
    {
      std::lock_guard<std::recursive_mutex> hl(imp->seg->host_mu);
      if (imp->seg->host != nullptr) {
        imp->seg->host->on_peer_disconnect(imp->key, imp->importer, false);
      }
    }
    delete imp;
    rs = RemoteSegment{};
  }

  Status interrupt(const RemoteSegment& rs, std::uint64_t imm) override {
    const auto* imp = static_cast<const Import*>(rs.handle);
    std::lock_guard<std::recursive_mutex> hl(imp->seg->host_mu);
    if (imp->seg->host == nullptr) {
      return Status::kDisconnected;
    }
    imp->seg->host->on_interrupt(imp->key, imp->importer, imm);
    return Status::kOk;
  }

  void stop() override {
    detach_host();
  }

private:
  void detach_host() {
    // Detach our host from our segments so that late interrupts are dropped.
    std::vector<std::shared_ptr<Segment>> mine;
    {
      auto& r = registry();
      std::lock_guard<std::mutex> lk(r.mu);
      for (auto& kv : r.segs) {
        if (kv.second->owner == this) {
          mine.push_back(kv.second);
        }
      }
    }
    for (auto& s : mine) {
      std::lock_guard<std::recursive_mutex> hl(s->host_mu);
      s->host = nullptr;
    }
  }

  static std::shared_ptr<Segment> find(NodeId node, std::uint32_t key) {
    auto& r = registry();
    std::lock_guard<std::mutex> lk(r.mu);
    const auto it = r.segs.find({node, key});
    return it == r.segs.end() ? nullptr : it->second;
  }

  PeerId self_;
  MappedHost* host_ = nullptr;
};

}  // namespace

std::unique_ptr<backend::MappedBackend> make_loopmem_backend() {
  return std::make_unique<LoopMemBackend>();
}

}  // namespace pushseg::backends
