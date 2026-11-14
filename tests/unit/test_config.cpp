#include "pushseg/config.hpp"
#include "pushseg/status.hpp"

#include <gtest/gtest.h>

namespace pushseg {
namespace {

TEST(ConfigTest, ParsesAllSections) {
  const Config cfg = Config::parse(R"(
# comment
[apps]
algo = 1
logger = 2   ; trailing comment

[node.0]
tcp = 10.0.0.10
mypci = bus3:0

[node.1]
tcp = 10.0.0.11:41000

[links]
default = tcp
1-0 = mypci

[backend.tcp]
base_port = 40000
)");
  EXPECT_EQ(cfg.app_id("algo"), 1);
  EXPECT_EQ(cfg.app_id("logger"), 2);
  EXPECT_EQ(cfg.nodes.at(0).at("mypci"), "bus3:0");
  EXPECT_EQ(cfg.nodes.at(1).at("tcp"), "10.0.0.11:41000");
  EXPECT_EQ(cfg.backend_for(0, 1), "mypci");
  EXPECT_EQ(cfg.backend_for(1, 0), "mypci");
  EXPECT_EQ(cfg.backend_for(0, 0), "tcp");
  EXPECT_EQ(cfg.backend_options.at("tcp").at("base_port"), "40000");
}

Status parse_error(const std::string& text) {
  try {
    (void)Config::parse(text);
  } catch (const Error& e) {
    return e.status();
  }
  return Status::kOk;
}

TEST(ConfigTest, RejectsMalformedInput) {
  EXPECT_EQ(parse_error("[apps]\nalgo = 300\n"), Status::kInvalidArgument);
  EXPECT_EQ(parse_error("[apps]\nalgo\n"), Status::kInvalidArgument);
  EXPECT_EQ(parse_error("[bogus]\n"), Status::kInvalidArgument);
  EXPECT_EQ(parse_error("[links]\n0x1 = tcp\n"), Status::kInvalidArgument);
  EXPECT_EQ(parse_error("algo = 1\n"), Status::kInvalidArgument);
  EXPECT_EQ(parse_error("[node.70000]\n"), Status::kInvalidArgument);
}

TEST(ConfigTest, UnknownAppThrowsNotFound) {
  const Config cfg = Config::parse("[apps]\na = 1\n");
  try {
    (void)cfg.app_id("b");
    FAIL() << "expected throw";
  } catch (const Error& e) {
    EXPECT_EQ(e.status(), Status::kNotFound);
  }
}

}  // namespace
}  // namespace pushseg
