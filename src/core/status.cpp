#include "pushseg/status.hpp"

namespace pushseg {

const char* to_string(Status s) noexcept {
  switch (s) {
    case Status::kOk:
      return "ok";
    case Status::kTimeout:
      return "timeout";
    case Status::kNotFound:
      return "not found";
    case Status::kExists:
      return "exists";
    case Status::kDisconnected:
      return "disconnected";
    case Status::kClosed:
      return "closed";
    case Status::kOutOfRange:
      return "out of range";
    case Status::kMisaligned:
      return "misaligned";
    case Status::kInvalidArgument:
      return "invalid argument";
    case Status::kUnsupported:
      return "unsupported";
    case Status::kProtocolError:
      return "protocol error";
    case Status::kIoError:
      return "I/O error";
  }
  return "unknown";
}

Error::Error(Status status, const std::string& what)
    : std::runtime_error(std::string(to_string(status)) + ": " + what), status_(status) {}

}  // namespace pushseg
