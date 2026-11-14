#include "pushseg/pushseg.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <set>

#include "core/internal.hpp"

namespace pushseg {
namespace detail {

namespace {
constexpr std::size_t kPage = 4096;
}  // namespace

void atomic_store_local(std::uint8_t* p, std::uint64_t bits, std::size_t width) noexcept {
  switch (width) {
    case 1:
      __atomic_store_n(p, static_cast<std::uint8_t>(bits), __ATOMIC_RELEASE);
      break;
    case 2:
      __atomic_store_n(
          reinterpret_cast<std::uint16_t*>(p), static_cast<std::uint16_t>(bits), __ATOMIC_RELEASE);
      break;
    case 4:
      __atomic_store_n(
          reinterpret_cast<std::uint32_t*>(p), static_cast<std::uint32_t>(bits), __ATOMIC_RELEASE);
      break;
    default:
      __atomic_store_n(reinterpret_cast<std::uint64_t*>(p), bits, __ATOMIC_RELEASE);
      break;
  }
}

// ---------------------------------------------------------------------------
// SegImpl
// ---------------------------------------------------------------------------

SegImpl::SegImpl(std::shared_ptr<ContextImpl> ctx,
                 SegKey key,
                 std::uint8_t* base,
                 std::size_t size,
                 bool mapped_alloc)
    : ctx_(std::move(ctx)), key_(key), base_(base), size_(size), mapped_alloc_(mapped_alloc) {}

SegImpl::~SegImpl() {
  ctx_->seg_released(key_, base_, mapped_alloc_);
}

void SegImpl::publish() {
  if (destroyed()) {
    throw Error(Status::kClosed, "segment destroyed");
  }
  if (!published_.exchange(true)) {
    try {
      ctx_->seg_published(*this);
    } catch (...) {
      published_.store(false);
      throw;
    }
  }
}

void SegImpl::unpublish() {
  if (published_.exchange(false)) {
    ctx_->seg_unpublished(*this);
  }
}

void SegImpl::destroy() {
  if (destroyed_.exchange(true)) {
    return;
  }
  unpublish();
  std::vector<std::weak_ptr<backend::Link>> links;
  {
    std::lock_guard<std::mutex> lk(mu_);
    links.swap(links_);
  }
  for (auto& w : links) {
    if (auto l = w.lock()) {
      l->close();
    }
  }
  cv_.notify_all();
}

void SegImpl::push_event(const Event& ev) {
  std::shared_ptr<std::function<void(const Event&)>> h;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (handler_) {
      h = handler_;
    } else {
      events_.push_back(ev);
    }
  }
  if (h) {
    (*h)(ev);
  } else {
    cv_.notify_one();
  }
}

Status SegImpl::wait_event(Event& ev, std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lk(mu_);
  cv_.wait_for(lk, timeout, [&] { return !events_.empty() || destroyed(); });
  if (!events_.empty()) {
    ev = events_.front();
    events_.pop_front();
    return Status::kOk;
  }
  return destroyed() ? Status::kClosed : Status::kTimeout;
}

bool SegImpl::poll_event(Event& ev) {
  std::lock_guard<std::mutex> lk(mu_);
  if (events_.empty()) {
    return false;
  }
  ev = events_.front();
  events_.pop_front();
  return true;
}

void SegImpl::set_handler(std::function<void(const Event&)> fn) {
  std::lock_guard<std::mutex> lk(mu_);
  if (fn) {
    handler_ = std::make_shared<std::function<void(const Event&)>>(std::move(fn));
  } else {
    handler_.reset();
  }
}

void SegImpl::add_link(const std::shared_ptr<backend::Link>& link) {
  bool close_now = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (destroyed()) {
      close_now = true;
    } else {
      // Drop entries of links that are already gone.
      links_.erase(
          std::remove_if(links_.begin(),
                         links_.end(),
                         [](const std::weak_ptr<backend::Link>& w) { return w.expired(); }),
          links_.end());
      links_.push_back(link);
    }
  }
  if (close_now) {
    link->close();
  }
}

// ---------------------------------------------------------------------------
// ContextImpl
// ---------------------------------------------------------------------------

ContextImpl::ContextImpl(Config cfg, PeerId self) : cfg_(std::move(cfg)), self_(self) {}

ContextImpl::~ContextImpl() {
  shutdown();
}

backend::Endpoint ContextImpl::endpoint(NodeId node,
                                        AppId app,
                                        InstId inst,
                                        const std::string& be) const {
  backend::Endpoint ep;
  ep.node = node;
  ep.app = app;
  ep.inst = inst;
  const auto n = cfg_.nodes.find(node);
  if (n != cfg_.nodes.end()) {
    const auto a = n->second.find(be);
    if (a != n->second.end()) {
      ep.address = a->second;
    }
  }
  return ep;
}

void ContextImpl::start() {
  if (cfg_.nodes.find(self_.node) == cfg_.nodes.end()) {
    throw Error(Status::kInvalidArgument,
                "node " + std::to_string(self_.node) + " is not in the config");
  }
  std::set<std::string> names;
  for (const auto& n : cfg_.nodes) {
    names.insert(cfg_.backend_for(self_.node, n.first));
  }
  try {
    for (const auto& name : names) {
      const backend::Endpoint self_ep = endpoint(self_.node, self_.app, self_.inst, name);
      const auto opt_it = cfg_.backend_options.find(name);
      const backend::Options opts =
          opt_it == cfg_.backend_options.end() ? backend::Options{} : opt_it->second;
      if (auto sb = make_stream_backend(name)) {
        const Status st = sb->start(self_ep, opts, *this);
        if (st != Status::kOk) {
          throw Error(st, "backend '" + name + "' failed to start");
        }
        streams_[name] = std::move(sb);
      } else if (auto mb = make_mapped_backend(name)) {
        if (mapped_) {
          throw Error(Status::kInvalidArgument,
                      "node uses two mapped backends ('" + mapped_name_ + "', '" + name + "')");
        }
        const Status st = mb->start(self_ep, opts, *this);
        if (st != Status::kOk) {
          throw Error(st, "backend '" + name + "' failed to start");
        }
        mapped_name_ = name;
        mapped_ = std::move(mb);
      } else {
        throw Error(Status::kInvalidArgument, "unknown backend '" + name + "'");
      }
    }
  } catch (...) {
    shutdown();
    throw;
  }
}

void ContextImpl::shutdown() {
  if (stopped_.exchange(true)) {
    return;
  }
  std::vector<std::shared_ptr<ConnImpl>> conns;
  std::vector<std::shared_ptr<SegImpl>> segs;
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& w : conns_) {
      if (auto c = w.lock()) {
        conns.push_back(std::move(c));
      }
    }
    conns_.clear();
    for (auto& s : segments_) {
      if (auto p = s.second.lock()) {
        segs.push_back(std::move(p));
      }
    }
  }
  for (auto& c : conns) {
    c->disconnect();
  }
  for (auto& s : segs) {
    s->destroy();
  }
  segs.clear();  // may release segments; seg_released() takes mu_
  for (auto& s : streams_) {
    s.second->stop();
  }
  if (mapped_) {
    mapped_->stop();
  }
}

std::shared_ptr<SegImpl> ContextImpl::create_segment(SegNo seg, std::size_t size) {
  if (stopped()) {
    throw Error(Status::kClosed, "context is shut down");
  }
  if (size == 0) {
    throw Error(Status::kInvalidArgument, "segment size must be > 0");
  }
  const SegKey key{self_.app, self_.inst, seg};
  // create_mu_ serializes creators; mu_ is only held for map access, never
  // across a backend call (backends call back into us under their own locks).
  std::lock_guard<std::mutex> create_lk(create_mu_);
  {
    std::lock_guard<std::mutex> lk(mu_);
    const auto it = segments_.find(key.raw());
    if (it != segments_.end() && !it->second.expired()) {
      throw Error(Status::kExists, "segment " + std::to_string(seg) + " already exists");
    }
  }
  void* base = nullptr;
  if (mapped_) {
    const Status st = mapped_->alloc_segment(key.raw(), size, &base);
    if (st != Status::kOk) {
      throw Error(st, "backend '" + mapped_name_ + "' could not allocate the segment");
    }
  } else {
    const std::size_t rounded = (size + kPage - 1) / kPage * kPage;
    base = std::aligned_alloc(kPage, rounded);
    if (base == nullptr) {
      throw Error(Status::kIoError, "out of memory");
    }
    std::memset(base, 0, rounded);
  }
  auto s = std::make_shared<SegImpl>(
      shared_from_this(), key, static_cast<std::uint8_t*>(base), size, mapped_ != nullptr);
  std::lock_guard<std::mutex> lk(mu_);
  segments_[key.raw()] = s;
  return s;
}

void ContextImpl::seg_published(const SegImpl& seg) {
  if (mapped_) {
    const Status st = mapped_->export_segment(seg.key().raw());
    if (st != Status::kOk) {
      throw Error(st, "backend '" + mapped_name_ + "' could not export the segment");
    }
  }
}

void ContextImpl::seg_unpublished(const SegImpl& seg) {
  if (mapped_) {
    mapped_->unexport_segment(seg.key().raw());
  }
}

void ContextImpl::seg_released(SegKey key, std::uint8_t* base, bool mapped_alloc) noexcept {
  {
    std::lock_guard<std::mutex> lk(mu_);
    const auto it = segments_.find(key.raw());
    if (it != segments_.end() && it->second.expired()) {
      segments_.erase(it);
    }
  }
  // Outside mu_: see create_segment().
  if (mapped_alloc) {
    mapped_->free_segment(key.raw());
  } else {
    std::free(base);
  }
}

std::shared_ptr<SegImpl> ContextImpl::find_segment(std::uint32_t key) {
  std::lock_guard<std::mutex> lk(mu_);
  const auto it = segments_.find(key);
  return it == segments_.end() ? nullptr : it->second.lock();
}

std::shared_ptr<SegImpl> ContextImpl::find_published(std::uint32_t key) {
  auto s = find_segment(key);
  return (s && s->published() && !s->destroyed()) ? s : nullptr;
}

std::shared_ptr<ConnImpl> ContextImpl::connect(NodeId node,
                                               SegKey key,
                                               std::chrono::milliseconds timeout) {
  if (stopped()) {
    throw Error(Status::kClosed, "context is shut down");
  }
  if (cfg_.nodes.find(node) == cfg_.nodes.end()) {
    throw Error(Status::kInvalidArgument, "node " + std::to_string(node) + " is not in the config");
  }
  const std::string& name = cfg_.backend_for(self_.node, node);
  const backend::Endpoint remote = endpoint(node, key.app, key.inst, name);
  const auto deadline = Clock::now() + timeout;

  std::shared_ptr<ConnImpl> conn;
  const auto sit = streams_.find(name);
  if (sit != streams_.end()) {
    conn = open_stream_conn(shared_from_this(), *sit->second, remote, key, deadline);
  } else if (mapped_ && name == mapped_name_) {
    conn = open_mapped_conn(shared_from_this(), *mapped_, remote, key, deadline);
  } else {
    throw Error(Status::kInvalidArgument, "backend '" + name + "' is not running");
  }

  std::lock_guard<std::mutex> lk(mu_);
  conns_.erase(std::remove_if(conns_.begin(),
                              conns_.end(),
                              [](const std::weak_ptr<ConnImpl>& w) { return w.expired(); }),
               conns_.end());
  conns_.push_back(conn);
  return conn;
}

std::shared_ptr<backend::LinkHandler> ContextImpl::on_accept(std::shared_ptr<backend::Link> link) {
  if (stopped()) {
    return nullptr;
  }
  return make_inbound_handler(shared_from_this(), std::move(link));
}

void ContextImpl::on_interrupt(std::uint32_t seg_key, const PeerId& from, std::uint64_t imm) {
  if (auto s = find_segment(seg_key)) {
    std::atomic_thread_fence(std::memory_order_acquire);
    s->push_event(Event{EventType::kNotify, from, imm});
  }
}

void ContextImpl::on_peer_connect(std::uint32_t seg_key, const PeerId& peer) {
  if (auto s = find_segment(seg_key)) {
    s->push_event(Event{EventType::kConnect, peer, 0});
  }
}

void ContextImpl::on_peer_disconnect(std::uint32_t seg_key, const PeerId& peer, bool lost) {
  if (auto s = find_segment(seg_key)) {
    s->push_event(Event{EventType::kDisconnect, peer, lost ? 1U : 0U});
  }
}

}  // namespace detail

// ---------------------------------------------------------------------------
// Public handles
// ---------------------------------------------------------------------------

LocalSegment::LocalSegment(std::shared_ptr<detail::SegImpl> impl) : impl_(std::move(impl)) {}

LocalSegment::~LocalSegment() {
  if (impl_) {
    impl_->destroy();
  }
}

LocalSegment& LocalSegment::operator=(LocalSegment&& other) noexcept {
  if (this != &other) {
    if (impl_) {
      impl_->destroy();
    }
    impl_ = std::move(other.impl_);
  }
  return *this;
}

void* LocalSegment::data() const noexcept {
  return impl_ ? impl_->base() : nullptr;
}

std::size_t LocalSegment::size() const noexcept {
  return impl_ ? impl_->size() : 0;
}

SegKey LocalSegment::key() const noexcept {
  return impl_ ? impl_->key() : SegKey{};
}

void LocalSegment::publish() {
  if (!impl_) {
    throw Error(Status::kClosed, "empty segment handle");
  }
  impl_->publish();
}

void LocalSegment::unpublish() {
  if (impl_) {
    impl_->unpublish();
  }
}

void LocalSegment::destroy() {
  if (impl_) {
    impl_->destroy();
    impl_.reset();
  }
}

Status LocalSegment::wait_event(Event& ev, std::chrono::milliseconds timeout) {
  return impl_ ? impl_->wait_event(ev, timeout) : Status::kClosed;
}

bool LocalSegment::poll_event(Event& ev) {
  return impl_ ? impl_->poll_event(ev) : false;
}

void LocalSegment::set_event_handler(std::function<void(const Event&)> fn) {
  if (impl_) {
    impl_->set_handler(std::move(fn));
  }
}

Connection::Connection(std::shared_ptr<detail::ConnImpl> impl) : impl_(std::move(impl)) {}

Connection::~Connection() {
  if (impl_) {
    impl_->disconnect();
  }
}

Connection& Connection::operator=(Connection&& other) noexcept {
  if (this != &other) {
    if (impl_) {
      impl_->disconnect();
    }
    impl_ = std::move(other.impl_);
  }
  return *this;
}

std::size_t Connection::size() const noexcept {
  return impl_ ? impl_->size() : 0;
}

Status Connection::write(std::uint64_t off, const void* src, std::size_t len) {
  return impl_ ? impl_->write(off, src, len) : Status::kClosed;
}

Status Connection::store_raw(std::uint64_t off, std::uint64_t bits, std::size_t width) {
  return impl_ ? impl_->store(off, bits, width) : Status::kClosed;
}

Status Connection::write_notify(std::uint64_t off,
                                const void* src,
                                std::size_t len,
                                std::uint64_t imm) {
  return impl_ ? impl_->write_notify(off, src, len, imm) : Status::kClosed;
}

Status Connection::notify(std::uint64_t imm) {
  return impl_ ? impl_->notify(imm) : Status::kClosed;
}

Status Connection::flush(std::chrono::milliseconds timeout) {
  return impl_ ? impl_->flush(timeout) : Status::kClosed;
}

void Connection::disconnect() {
  if (impl_) {
    impl_->disconnect();
  }
}

Context::Context(const Config& cfg, NodeId node, const std::string& app, InstId inst)
    : impl_(std::make_shared<detail::ContextImpl>(cfg, PeerId{node, cfg.app_id(app), inst})) {
  impl_->start();
}

Context::~Context() {
  if (impl_) {
    impl_->shutdown();
  }
}

Context& Context::operator=(Context&& other) noexcept {
  if (this != &other) {
    if (impl_) {
      impl_->shutdown();
    }
    impl_ = std::move(other.impl_);
  }
  return *this;
}

PeerId Context::self() const noexcept {
  return impl_ ? impl_->self() : PeerId{};
}

LocalSegment Context::create_segment(SegNo seg, std::size_t size) {
  if (!impl_) {
    throw Error(Status::kClosed, "empty context handle");
  }
  return LocalSegment(impl_->create_segment(seg, size));
}

Connection Context::connect(NodeId node, SegKey key, std::chrono::milliseconds timeout) {
  if (!impl_) {
    throw Error(Status::kClosed, "empty context handle");
  }
  return Connection(impl_->connect(node, key, timeout));
}

Connection Context::connect(NodeId node,
                            const std::string& app,
                            InstId inst,
                            SegNo seg,
                            std::chrono::milliseconds timeout) {
  if (!impl_) {
    throw Error(Status::kClosed, "empty context handle");
  }
  const SegKey key{impl_->config().app_id(app), inst, seg};
  return connect(node, key, timeout);
}

}  // namespace pushseg
