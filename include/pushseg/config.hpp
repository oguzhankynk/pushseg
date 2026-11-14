#ifndef PUSHSEG_CONFIG_HPP
#define PUSHSEG_CONFIG_HPP

#include "pushseg/types.hpp"

#include <map>
#include <string>
#include <utility>

namespace pushseg {

/// @brief System description: nodes, apps, which backend links which nodes.
///
/// Text format (INI-like, '#' or ';' start a comment):
/// @code
/// [apps]
/// algo = 1
///
/// [node.0]
/// tcp = 10.0.0.10          # backend name = backend-specific address
/// mypci = bus3:0
///
/// [node.1]
/// tcp = 10.0.0.11:41000    # tcp: optional per-node base port
///
/// [links]
/// default = tcp
/// 0-1 = mypci
///
/// [backend.tcp]
/// base_port = 40000
/// @endcode
struct Config {
  /// @brief Application name -> id.
  std::map<std::string, AppId> apps;
  /// @brief Node -> (backend name -> address string). The address is opaque to the core.
  std::map<NodeId, std::map<std::string, std::string>> nodes;
  /// @brief Backend name -> option map ([backend.NAME] sections).
  std::map<std::string, std::map<std::string, std::string>> backend_options;
  /// @brief Backend used for node pairs without an explicit [links] entry.
  std::string default_backend = "tcp";
  /// @brief Explicit links; key is normalized so that first <= second.
  std::map<std::pair<NodeId, NodeId>, std::string> links;

  /// @brief Parses the text format. Throws pushseg::Error(kInvalidArgument) on error.
  /// @param text Config file contents.
  /// @return Parsed config.
  static Config parse(const std::string& text);

  /// @brief Reads and parses a file. Throws pushseg::Error on error.
  /// @param path File path.
  /// @return Parsed config.
  static Config load(const std::string& path);

  /// @brief Backend name used between two nodes.
  /// @param a One node.
  /// @param b Other node (may equal a).
  /// @return Backend name.
  const std::string& backend_for(NodeId a, NodeId b) const;

  /// @brief Sets the backend for a node pair.
  /// @param a One node.
  /// @param b Other node.
  /// @param backend Backend name.
  void set_link(NodeId a, NodeId b, std::string backend);

  /// @brief Looks up an application id. Throws pushseg::Error(kNotFound).
  /// @param name Application name.
  /// @return Its id.
  AppId app_id(const std::string& name) const;
};

}  // namespace pushseg

#endif  // PUSHSEG_CONFIG_HPP
