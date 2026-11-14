#ifndef PUSHSEG_TYPES_HPP
#define PUSHSEG_TYPES_HPP

#include <cstdint>

namespace pushseg {

/// @brief Target (machine) number, from the config file.
using NodeId = std::uint16_t;
/// @brief Application number, from the config file's [apps] section.
using AppId = std::uint8_t;
/// @brief Instance of an application on one node.
using InstId = std::uint8_t;
/// @brief Segment number, chosen by the application. Unique within (app, inst).
using SegNo = std::uint16_t;

/// @brief Identifies one running process: (node, app, inst).
struct PeerId {
  NodeId node = 0;  ///< Node the process runs on.
  AppId app = 0;    ///< Application id.
  InstId inst = 0;  ///< Instance id.
};

/// @brief Equality of peers.
/// @param a Left operand.
/// @param b Right operand.
/// @return True when all fields match.
constexpr bool operator==(const PeerId& a, const PeerId& b) noexcept {
  return a.node == b.node && a.app == b.app && a.inst == b.inst;
}

/// @brief Segment key, unique on a node: [app:8][inst:8][seg:16].
///
/// Together with the owning NodeId this is the global address of a segment.
struct SegKey {
  AppId app = 0;    ///< Owning application.
  InstId inst = 0;  ///< Owning instance.
  SegNo seg = 0;    ///< Segment number inside the instance.

  /// @brief Packs the key into 32 bits.
  /// @return The packed key.
  constexpr std::uint32_t raw() const noexcept {
    return (std::uint32_t{app} << 24U) | (std::uint32_t{inst} << 16U) | std::uint32_t{seg};
  }

  /// @brief Unpacks a 32-bit key.
  /// @param r Packed key.
  /// @return The key.
  static constexpr SegKey from_raw(std::uint32_t r) noexcept {
    return SegKey{static_cast<AppId>(r >> 24U),
                  static_cast<InstId>((r >> 16U) & 0xFFU),
                  static_cast<SegNo>(r & 0xFFFFU)};
  }
};

/// @brief Kind of a segment event.
enum class EventType : std::uint8_t {
  kConnect,     ///< A writer connected.
  kDisconnect,  ///< A writer disconnected. imm: 0 = graceful, 1 = link lost.
  kNotify,      ///< A writer sent notify / write_notify. imm: user value.
};

/// @brief Event delivered to a segment's owner.
struct Event {
  EventType type = EventType::kNotify;  ///< What happened.
  PeerId peer;                          ///< Which writer.
  std::uint64_t imm = 0;                ///< Immediate value (see EventType).
};

}  // namespace pushseg

#endif  // PUSHSEG_TYPES_HPP
