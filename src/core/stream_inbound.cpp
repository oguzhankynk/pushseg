// Owner side of the stream protocol: applies incoming frames to a local segment.
//
// Frames of one link are processed strictly in order on the backend's
// delivery thread, which gives per-connection ordering for free. Atomicity:
// bulk payload is memcpy'd as it arrives (no promise); kStore values travel in
// the header, so they are applied only once complete, with a single release
// store. Notify events go through the segment's mutex-protected queue, which
// publishes all earlier writes to the thread that takes the event.

#include <atomic>
#include <cstring>

#include "core/internal.hpp"
#include "core/wire.hpp"

namespace pushseg::detail {
namespace {

class InboundLink final : public backend::LinkHandler, private FrameDecoder::Sink {
public:
  InboundLink(std::weak_ptr<ContextImpl> ctx, std::shared_ptr<backend::Link> link)
      : ctx_(std::move(ctx)), link_(std::move(link)) {}

  void on_recv(const void* data, std::size_t len) override {
    if (failed_) {
      return;
    }
    if (!dec_.feed(static_cast<const std::uint8_t*>(data), len, *this)) {
      failed_ = true;
      link_->close();
    }
  }

  void on_down(Status /*reason*/) override {
    if (seg_ && !disconnect_sent_) {
      seg_->push_event(Event{EventType::kDisconnect, peer_, 1});
    }
    seg_.reset();
    link_.reset();
  }

private:
  bool on_header(const FrameHeader& h) override {
    if (h.op == Op::kConnect) {
      return handle_connect(h);
    }
    if (!seg_ || seg_->destroyed()) {
      return false;  // data before a successful connect, or segment gone
    }
    switch (h.op) {
      case Op::kWrite:
      case Op::kWriteNotify:
        return check_range(h.off, h.len, seg_->size()) == Status::kOk;
      case Op::kStore:
        if (h.len != 0 || check_store(h.off, h.aux, seg_->size()) != Status::kOk) {
          return false;
        }
        atomic_store_local(seg_->base() + h.off, h.imm, h.aux);
        return true;
      case Op::kNotify:
      case Op::kFence:
      case Op::kDisconnect:
        return h.len == 0;
      default:
        return false;
    }
  }

  bool on_payload(const FrameHeader& h,
                  std::uint64_t pos,
                  const std::uint8_t* p,
                  std::size_t n) override {
    std::memcpy(seg_->base() + h.off + pos, p, n);
    return true;
  }

  bool on_end(const FrameHeader& h) override {
    switch (h.op) {
      case Op::kWriteNotify:
      case Op::kNotify:
        std::atomic_thread_fence(std::memory_order_release);
        seg_->push_event(Event{EventType::kNotify, peer_, h.imm});
        return true;
      case Op::kFence:
        return reply(Op::kFenceAck, 0, h.imm);
      case Op::kDisconnect:
        disconnect_sent_ = true;
        seg_->push_event(Event{EventType::kDisconnect, peer_, 0});
        seg_.reset();
        link_->close();
        return true;
      default:
        return true;
    }
  }

  bool handle_connect(const FrameHeader& h) {
    if (seg_ || h.len != 0) {
      return false;
    }
    auto ctx = ctx_.lock();
    auto seg = ctx ? ctx->find_published(h.aux) : nullptr;
    if (!seg) {
      // Not (yet) published: the writer retries on the same link.
      return reply(Op::kConnectErr, static_cast<std::uint32_t>(Status::kNotFound), 0);
    }
    peer_ = decode_peer(h.imm);
    seg_ = std::move(seg);
    seg_->add_link(link_);
    if (!reply(Op::kConnectOk, 0, seg_->size())) {
      return false;
    }
    seg_->push_event(Event{EventType::kConnect, peer_, 0});
    return true;
  }

  bool reply(Op op, std::uint32_t aux, std::uint64_t imm) {
    FrameHeader r;
    r.op = op;
    r.aux = aux;
    r.imm = imm;
    iovec iov{&r, sizeof(r)};
    return link_->sendv(&iov, 1) == Status::kOk;
  }

  std::weak_ptr<ContextImpl> ctx_;
  std::shared_ptr<backend::Link> link_;
  std::shared_ptr<SegImpl> seg_;
  PeerId peer_;
  FrameDecoder dec_;
  bool failed_ = false;
  bool disconnect_sent_ = false;
};

}  // namespace

std::shared_ptr<backend::LinkHandler> make_inbound_handler(std::shared_ptr<ContextImpl> ctx,
                                                           std::shared_ptr<backend::Link> link) {
  return std::make_shared<InboundLink>(ctx, std::move(link));
}

}  // namespace pushseg::detail
