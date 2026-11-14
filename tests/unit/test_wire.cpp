#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "core/wire.hpp"

namespace pushseg::detail {
namespace {

struct Recorder : FrameDecoder::Sink {
  std::vector<FrameHeader> headers;
  std::string payload;
  int ends = 0;
  bool on_header(const FrameHeader& h) override {
    headers.push_back(h);
    return true;
  }
  bool on_payload(const FrameHeader& /*h*/,
                  std::uint64_t pos,
                  const std::uint8_t* p,
                  std::size_t n) override {
    EXPECT_EQ(pos, payload.size());
    payload.append(reinterpret_cast<const char*>(p), n);
    return true;
  }
  bool on_end(const FrameHeader& /*h*/) override {
    ++ends;
    return true;
  }
};

std::vector<std::uint8_t> frame(Op op, const std::string& body, std::uint64_t imm) {
  FrameHeader h;
  h.op = op;
  h.len = static_cast<std::uint32_t>(body.size());
  h.imm = imm;
  std::vector<std::uint8_t> out(sizeof(h) + body.size());
  std::memcpy(out.data(), &h, sizeof(h));
  std::memcpy(out.data() + sizeof(h), body.data(), body.size());
  return out;
}

TEST(WireTest, DecodesFramesSplitAtEveryByte) {
  auto bytes = frame(Op::kWrite, "hello world", 0);
  const auto b2 = frame(Op::kNotify, "", 7);
  bytes.insert(bytes.end(), b2.begin(), b2.end());

  Recorder r;
  FrameDecoder d;
  for (std::uint8_t b : bytes) {
    ASSERT_TRUE(d.feed(&b, 1, r));
  }
  ASSERT_EQ(r.headers.size(), 2U);
  EXPECT_EQ(r.headers[0].op, Op::kWrite);
  EXPECT_EQ(r.headers[1].op, Op::kNotify);
  EXPECT_EQ(r.headers[1].imm, 7U);
  EXPECT_EQ(r.payload, "hello world");
  EXPECT_EQ(r.ends, 2);
}

TEST(WireTest, RejectsBadMagic) {
  auto bytes = frame(Op::kNotify, "", 0);
  bytes[0] ^= 0xFFU;
  Recorder r;
  FrameDecoder d;
  EXPECT_FALSE(d.feed(bytes.data(), bytes.size(), r));
}

TEST(WireTest, PeerRoundTrip) {
  const PeerId p{513, 7, 3};
  EXPECT_EQ(decode_peer(encode_peer(p)), p);
}

}  // namespace
}  // namespace pushseg::detail
