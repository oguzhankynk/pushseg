#include <mutex>

#include <emmintrin.h>

#include "core/internal.hpp"

namespace pushseg {
namespace backends {
// Built-in backends (src/backends/).
std::unique_ptr<backend::StreamBackend> make_tcp_backend();
std::unique_ptr<backend::MappedBackend> make_loopmem_backend();
std::unique_ptr<backend::MappedBackend> make_shm_backend();
std::unique_ptr<backend::MappedBackend> make_ntb_backend();
}  // namespace backends

namespace backend {

// Default implementations of the optional MappedBackend hooks.
void MappedBackend::store_barrier(const RemoteSegment& /*seg*/) {
  _mm_sfence();
}

Status MappedBackend::flush(const RemoteSegment& seg) {
  store_barrier(seg);
  return Status::kOk;
}

Status MappedBackend::dma_write(const RemoteSegment& /*seg*/,
                                std::uint64_t /*off*/,
                                const void* /*src*/,
                                std::size_t /*len*/) {
  return Status::kUnsupported;
}

Status MappedBackend::dma_wait(const RemoteSegment& /*seg*/) {
  return Status::kOk;
}

}  // namespace backend

namespace detail {
namespace {

struct Registry {
  std::mutex mu;
  std::map<std::string, backend::StreamFactory> streams;
  std::map<std::string, backend::MappedFactory> mapped;
};

// Built-ins are registered on first use rather than by static initializers,
// which the linker would drop from a static library.
Registry& registry() {
  static Registry* const r = [] {
    auto* reg = new Registry();
    reg->streams["tcp"] = backends::make_tcp_backend;
    reg->mapped["loopmem"] = backends::make_loopmem_backend;
    reg->mapped["shm"] = backends::make_shm_backend;
    reg->mapped["ntb"] = backends::make_ntb_backend;
    return reg;
  }();
  return *r;
}

}  // namespace

std::unique_ptr<backend::StreamBackend> make_stream_backend(const std::string& name) {
  Registry& r = registry();
  std::lock_guard<std::mutex> lk(r.mu);
  const auto it = r.streams.find(name);
  return it == r.streams.end() ? nullptr : it->second();
}

std::unique_ptr<backend::MappedBackend> make_mapped_backend(const std::string& name) {
  Registry& r = registry();
  std::lock_guard<std::mutex> lk(r.mu);
  const auto it = r.mapped.find(name);
  return it == r.mapped.end() ? nullptr : it->second();
}

}  // namespace detail

namespace backend {

void register_stream_backend(const std::string& name, StreamFactory factory) {
  auto& r = detail::registry();
  std::lock_guard<std::mutex> lk(r.mu);
  r.mapped.erase(name);
  r.streams[name] = std::move(factory);
}

void register_mapped_backend(const std::string& name, MappedFactory factory) {
  auto& r = detail::registry();
  std::lock_guard<std::mutex> lk(r.mu);
  r.streams.erase(name);
  r.mapped[name] = std::move(factory);
}

}  // namespace backend
}  // namespace pushseg
