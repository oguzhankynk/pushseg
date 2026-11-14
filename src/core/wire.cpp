#include "core/wire.hpp"

#include <algorithm>
#include <cstring>

namespace pushseg::detail {

bool FrameDecoder::feed(const std::uint8_t* p, std::size_t n, Sink& sink) {
  while (n > 0) {
    if (!in_payload_) {
      const std::size_t take = std::min(sizeof(FrameHeader) - hhave_, n);
      std::memcpy(hbuf_ + hhave_, p, take);
      hhave_ += take;
      p += take;
      n -= take;
      if (hhave_ < sizeof(FrameHeader)) {
        return true;
      }
      hhave_ = 0;
      std::memcpy(&hdr_, hbuf_, sizeof(FrameHeader));
      if (hdr_.magic != kWireMagic || hdr_.version != kWireVersion) {
        return false;
      }
      if (!sink.on_header(hdr_)) {
        return false;
      }
      if (hdr_.len == 0) {
        if (!sink.on_end(hdr_)) {
          return false;
        }
      } else {
        in_payload_ = true;
        pos_ = 0;
      }
    } else {
      const std::size_t take = std::min<std::uint64_t>(hdr_.len - pos_, n);
      if (!sink.on_payload(hdr_, pos_, p, take)) {
        return false;
      }
      pos_ += take;
      p += take;
      n -= take;
      if (pos_ == hdr_.len) {
        in_payload_ = false;
        if (!sink.on_end(hdr_)) {
          return false;
        }
      }
    }
  }
  return true;
}

}  // namespace pushseg::detail
