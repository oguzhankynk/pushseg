#include "pushseg/config.hpp"

#include "pushseg/status.hpp"

#include <cctype>
#include <fstream>
#include <limits>
#include <sstream>

namespace pushseg {
namespace {

std::string trim(const std::string& s) {
  std::size_t b = 0;
  std::size_t e = s.size();
  while (b < e && std::isspace(static_cast<unsigned char>(s[b])) != 0) {
    ++b;
  }
  while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1])) != 0) {
    --e;
  }
  return s.substr(b, e - b);
}

[[noreturn]] void fail(int line, const std::string& msg) {
  throw Error(Status::kInvalidArgument, "config line " + std::to_string(line) + ": " + msg);
}

unsigned long parse_uint(const std::string& s, unsigned long max, int line) {
  if (s.empty()) {
    fail(line, "expected a number");
  }
  for (char c : s) {
    if (std::isdigit(static_cast<unsigned char>(c)) == 0) {
      fail(line, "not a number: '" + s + "'");
    }
  }
  unsigned long v = 0;
  try {
    v = std::stoul(s);
  } catch (const std::exception&) {
    fail(line, "number out of range: '" + s + "'");
  }
  if (v > max) {
    fail(line, "number too large: '" + s + "'");
  }
  return v;
}

std::pair<NodeId, NodeId> norm(NodeId a, NodeId b) {
  return a <= b ? std::make_pair(a, b) : std::make_pair(b, a);
}

}  // namespace

Config Config::parse(const std::string& text) {
  Config cfg;
  std::istringstream in(text);
  std::string raw;
  std::string section;
  int line = 0;
  constexpr unsigned long kMaxNode = std::numeric_limits<NodeId>::max();
  while (std::getline(in, raw)) {
    ++line;
    const std::size_t hash = raw.find_first_of("#;");
    const std::string l = trim(hash == std::string::npos ? raw : raw.substr(0, hash));
    if (l.empty()) {
      continue;
    }
    if (l.front() == '[') {
      if (l.back() != ']') {
        fail(line, "unterminated section header");
      }
      section = trim(l.substr(1, l.size() - 2));
      if (section.rfind("node.", 0) == 0) {
        cfg.nodes[static_cast<NodeId>(parse_uint(section.substr(5), kMaxNode, line))];
      } else if (section.rfind("backend.", 0) == 0) {
        cfg.backend_options[section.substr(8)];
      } else if (section != "apps" && section != "links") {
        fail(line, "unknown section [" + section + "]");
      }
      continue;
    }
    const std::size_t eq = l.find('=');
    if (eq == std::string::npos) {
      fail(line, "expected key = value");
    }
    const std::string key = trim(l.substr(0, eq));
    const std::string val = trim(l.substr(eq + 1));
    if (key.empty()) {
      fail(line, "empty key");
    }
    if (section == "apps") {
      cfg.apps[key] = static_cast<AppId>(parse_uint(val, std::numeric_limits<AppId>::max(), line));
    } else if (section.rfind("node.", 0) == 0) {
      const auto node = static_cast<NodeId>(parse_uint(section.substr(5), kMaxNode, line));
      cfg.nodes[node][key] = val;
    } else if (section.rfind("backend.", 0) == 0) {
      cfg.backend_options[section.substr(8)][key] = val;
    } else if (section == "links") {
      if (val.empty()) {
        fail(line, "empty backend name");
      }
      if (key == "default") {
        cfg.default_backend = val;
        continue;
      }
      const std::size_t dash = key.find('-');
      if (dash == std::string::npos) {
        fail(line, "link key must be 'A-B' or 'default'");
      }
      const auto a = static_cast<NodeId>(parse_uint(trim(key.substr(0, dash)), kMaxNode, line));
      const auto b = static_cast<NodeId>(parse_uint(trim(key.substr(dash + 1)), kMaxNode, line));
      cfg.set_link(a, b, val);
    } else {
      fail(line, "key outside of a section");
    }
  }
  return cfg;
}

Config Config::load(const std::string& path) {
  std::ifstream f(path);
  if (!f) {
    throw Error(Status::kNotFound, "cannot open config file '" + path + "'");
  }
  std::ostringstream ss;
  ss << f.rdbuf();
  return parse(ss.str());
}

const std::string& Config::backend_for(NodeId a, NodeId b) const {
  const auto it = links.find(norm(a, b));
  return it == links.end() ? default_backend : it->second;
}

void Config::set_link(NodeId a, NodeId b, std::string backend) {
  links[norm(a, b)] = std::move(backend);
}

AppId Config::app_id(const std::string& name) const {
  const auto it = apps.find(name);
  if (it == apps.end()) {
    throw Error(Status::kNotFound, "unknown application '" + name + "'");
  }
  return it->second;
}

}  // namespace pushseg
