set -euxo pipefail

# === Get Directory Paths ===
ROOT_DIR=$(dirname "$(dirname "$(readlink -f "$0")")")

DEPS_DIR=$ROOT_DIR/build/_deps
ARROW_INSTALL_DIR=$DEPS_DIR/arrow-install
ARROW_BUILD_DIR=$DEPS_DIR/arrow-prefix/src/arrow-build
PROTOBUF_INSTALL_DIR=$ARROW_BUILD_DIR/protobuf_ep-install
GRAPHAR_CLI_DIR=$DEPS_DIR/graphar-prefix/src/graphar/cli

# === Build CMake arguments for pip ===
CMAKE_ARGS=(
  # Path to packages
  --config-settings=cmake.define.Arrow_DIR="$ARROW_INSTALL_DIR/lib/cmake/Arrow"
  --config-settings=cmake.define.Parquet_DIR="$ARROW_INSTALL_DIR/lib/cmake/Parquet"
  --config-settings=cmake.define.ArrowDataset_DIR="$ARROW_INSTALL_DIR/lib/cmake/ArrowDataset"
  --config-settings=cmake.define.ArrowAcero_DIR="$ARROW_INSTALL_DIR/lib/cmake/ArrowAcero"
  --config-settings=cmake.define.Protobuf_INCLUDE_DIR="$PROTOBUF_INSTALL_DIR/include"
  --config-settings=cmake.define.Protobuf_LIBRARIES="$PROTOBUF_INSTALL_DIR/lib/libprotobuf.a"

  # Enable RPATH to link against libraries in install directory
  --config-settings=cmake.define.CMAKE_INSTALL_RPATH_USE_LINK_PATH=ON
)

# === Install the package ===
pip3 install "$GRAPHAR_CLI_DIR" "${CMAKE_ARGS[@]}"