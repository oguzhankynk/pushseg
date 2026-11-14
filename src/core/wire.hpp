#ifndef PUSHSEG_SRC_CORE_WIRE_HPP
#define PUSHSEG_SRC_CORE_WIRE_HPP

// Segment protocol run by the core over any StreamBackend.
//
// Every frame is a fixed 32-byte header followed by `len` payload bytes.
// Host byte order (x86_64 little endian only).
//
//   writer -> owner                     owner -> writer
//   ---------------                     ---------------
//   kConnect   aux=seg_key imm=peer     kConnectOk  imm=segment size
//   kWrite     off len +payload         kConnectErr aux=Status
//   kStore     off aux=width imm=value  kFenceAck   imm=fence id
//   kWriteNotify off len imm +payload
//   kNotify    imm
//   kFence     imm=fence id
//   kDisconnect

#include "pushseg/types.hpp"

#include <cstddef>
#include <cstdint>

namespace pushseg::detail {

inline constexpr std::uint16_t kWireMagic = 0x5350;  // "PS" in memory order
inline constexpr std::uint8_t kWireVersion = 1;
// Large writes are split into frames of at most this many payload bytes.
inline constexpr std::size_t kMaxFramePayload = std::size_t{1} << 24U;

enum class Op : std::uint8_t {
  kConnect = 1,
  kConnectOk,
  kConnectErr,
  kWrite,
  kStore,
  kWriteNotify,
  kNotify,
  kFence,
  kFenceAck,
  kDisconnect,
};

struct FrameHeader {
  std::uint16_t magic = kWireMagic;
  std::uint8_t version = kWireVersion;
  Op op = Op::kNotify;
  std::uint32_t len = 0;  // payload bytes
  std::uint64_t off = 0;
  std::uint64_t imm = 0;
  std::uint32_t aux = 0;
  std::uint32_t reserved = 0;
};
static_assert(sizeof(FrameHeader) == 32, "wire header must be 32 bytes");

inline std::uint64_t encode_peer(const PeerId& p) noexcept {
  return std::uint64_t{p.node} | (std::uint64_t{p.app} << 16U) | (std::uint64_t{p.inst} << 24U);
}

inline PeerId decode_peer(std::uint64_t v) noexcept {
  return PeerId{static_cast<NodeId>(v & 0xFFFFU),
                static_cast<AppId>((v >> 16U) & 0xFFU),
                static_cast<InstId>((v >> 24U) & 0xFFU)};
}

// Incremental frame parser. Feed it bytes in any chunking.
class FrameDecoder {
public:
  class Sink {
  public:
    virtual ~Sink() = default;
    // Header complete. Return false to abort (protocol error).
    virtual bool on_header(const FrameHeader& h) = 0;
    // A slice of the payload starting at payload position `pos`.
    virtual bool on_payload(const FrameHeader& h,
                            std::uint64_t pos,
                            const std::uint8_t* p,
                            std::size_t n) = 0;
    // Frame complete (also called for len == 0 frames).
    virtual bool on_end(const FrameHeader& h) = 0;
  };

  // Returns false on a protocol error; the decoder must not be fed again.
  bool feed(const std::uint8_t* p, std::size_t n, Sink& sink);

private:
  FrameHeader hdr_{};
  std::uint8_t hbuf_[sizeof(FrameHeader)] = {};
  std::size_t hhave_ = 0;
  std::uint64_t pos_ = 0;
  bool in_payload_ = false;
};

}  // namespace pushseg::detail

#endif  // PUSHSEG_SRC_CORE_WIRE_HPP
