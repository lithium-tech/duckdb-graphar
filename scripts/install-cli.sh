ROOTDIR=$(dirname "$(dirname "$(readlink -f "$0")")")

pip3 install ./build/_deps/graphar-prefix/src/graphar/cli \
  --config-settings=cmake.define.Arrow_DIR="$ROOTDIR/build/_deps/arrow-install/lib/cmake/Arrow" \
  --config-settings=cmake.define.Parquet_DIR="$ROOTDIR/build/_deps/arrow-install/lib/cmake/Parquet" \
  --config-settings=cmake.define.ArrowDataset_DIR="$ROOTDIR/build/_deps/arrow-install/lib/cmake/ArrowDataset" \
  --config-settings=cmake.define.ArrowAcero_DIR="$ROOTDIR/build/_deps/arrow-install/lib/cmake/ArrowAcero" \
  --config-settings=cmake.define.Protobuf_INCLUDE_DIR="$ROOTDIR/build/_deps/arrow-prefix/src/arrow-build/protobuf_ep-install/include" \
  --config-settings=cmake.define.Protobuf_LIBRARIES="$ROOTDIR/build/_deps/arrow-prefix/src/arrow-build/protobuf_ep-install/lib/libprotobuf.a" \
  --config-settings=cmake.define.CMAKE_SHARED_LINKER_FLAGS="-Wl,--disable-new-dtags"
