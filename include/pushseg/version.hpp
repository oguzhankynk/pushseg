#ifndef PUSHSEG_VERSION_HPP
#define PUSHSEG_VERSION_HPP

/// @brief pushseg: segment-based, push-only data transfer between targets.
namespace pushseg {

/// @brief Returns the library version as "MAJOR.MINOR.PATCH".
/// @return A pointer to a static, null-terminated string. Never null.
const char* version() noexcept;

}  // namespace pushseg

#endif  // PUSHSEG_VERSION_HPP
