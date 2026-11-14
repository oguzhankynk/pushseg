#ifndef PUSHSEG_STATUS_HPP
#define PUSHSEG_STATUS_HPP

#include <stdexcept>
#include <string>

namespace pushseg {

/// @brief Result code of an pushseg operation.
enum class Status : int {
  kOk = 0,           ///< Success.
  kTimeout,          ///< The deadline passed before the operation completed.
  kNotFound,         ///< The remote segment does not exist or is not published.
  kExists,           ///< A segment with the same key already exists.
  kDisconnected,     ///< The connection broke (peer gone, link error).
  kClosed,           ///< The handle was closed locally.
  kOutOfRange,       ///< Offset/length exceed the segment.
  kMisaligned,       ///< Atomic store offset is not aligned to its width.
  kInvalidArgument,  ///< Bad argument or configuration.
  kUnsupported,      ///< The backend does not support this operation.
  kProtocolError,    ///< The peer sent something malformed.
  kIoError,          ///< An OS-level I/O error occurred.
};

/// @brief Human-readable name of a status code.
/// @param s Status to describe.
/// @return Static string, never null.
const char* to_string(Status s) noexcept;

/// @brief Exception thrown by setup calls (context, segment, connect).
class Error : public std::runtime_error {
public:
  /// @brief Creates an error.
  /// @param status Machine-readable code.
  /// @param what Description.
  Error(Status status, const std::string& what);

  /// @brief The machine-readable code.
  /// @return Status carried by this error.
  Status status() const noexcept {
    return status_;
  }

private:
  Status status_;
};

}  // namespace pushseg

#endif  // PUSHSEG_STATUS_HPP
