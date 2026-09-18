export CMAKE_POLICY_VERSION_MINIMUM=3.5
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# cmake --build uses make's -j (deferred eval: MAKEFLAGS is set at run time).
MAKE_JOBS = $(if $(shell echo "$(MAKEFLAGS)" | sed -n 's/.*-j *\([0-9][0-9]*\).*/\1/p' | head -1),$(shell echo "$(MAKEFLAGS)" | sed -n 's/.*-j *\([0-9][0-9]*\).*/\1/p' | head -1),$(shell getconf _NPROCESSORS_ONLN))
export CMAKE_BUILD_PARALLEL_LEVEL = $(MAKE_JOBS)

# Configuration of extension
EXT_NAME=graphar_duck
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

THIRD_PARTY_DIR=$(PROJ_DIR)third_party
THIRD_PARTY_CMAKE=$(PROJ_DIR)third_party/extension_deps.cmake

BOOST_VERSION=1.92.0
BOOST_VERSION_UNDERSCORE=1_92_0
BOOST_DIR=$(THIRD_PARTY_DIR)/boost
BOOST_INSTALL_DIR=$(BOOST_DIR)/install
BOOST_SRC_DIR=$(BOOST_DIR)/src
BOOST_ARCHIVE=$(BOOST_DIR)/boost_$(BOOST_VERSION_UNDERSCORE).tar.gz
BOOST_URL=https://archives.boost.io/release/$(BOOST_VERSION)/source/boost_$(BOOST_VERSION_UNDERSCORE).tar.gz
BOOST_INSTALLED=$(BOOST_DIR)/.installed-$(BOOST_VERSION)
BOOST_CMAKE_DIR=$(BOOST_INSTALL_DIR)/lib/cmake/Boost-$(BOOST_VERSION)

ARROW_REP=https://github.com/apache/arrow.git
ARROW_VERSION=23.0.0
ARROW_DIR=$(THIRD_PARTY_DIR)/arrow
ARROW_INSTALL_DIR=$(ARROW_DIR)/install
ARROW_SRC_DIR=$(ARROW_DIR)/src
ARROW_BUILD_DIR=$(ARROW_SRC_DIR)/cpp/build
ARROW_CLONED = $(ARROW_DIR)/.cloned
ARROW_BUILT = $(ARROW_DIR)/.built
ARROW_INSTALLED = $(ARROW_DIR)/.installed

GRAPHAR_REP=https://github.com/lithium-tech/incubator-graphar.git
GRAPHAR_COMMIT=8a4c3c9633b5e130812c5cb79171beebdcc4ad42
GRAPHAR_DIR=$(THIRD_PARTY_DIR)/graphar
GRAPHAR_INSTALL_DIR=$(GRAPHAR_DIR)/install
GRAPHAR_SRC_DIR=$(GRAPHAR_DIR)/src
GRAPHAR_BUILD_DIR=$(GRAPHAR_SRC_DIR)/cpp/build
GRAPHAR_CLONED = $(GRAPHAR_DIR)/.cloned
GRAPHAR_BUILT = $(GRAPHAR_DIR)/.built
GRAPHAR_INSTALLED = $(GRAPHAR_DIR)/.installed

ARROW_ROOT=$(ARROW_INSTALL_DIR)
GRAPHAR_ROOT=$(GRAPHAR_INSTALL_DIR)

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Two test suites: SQL (test/sql/, via DuckDB's `unittest`) and C++ unit tests
# (test/cpp/, own binary `unittest_graphar`). Both are built by default.
EXT_RELEASE_FLAGS += -DBUILD_EXTENSION_UNIT_TESTS=ON -DBUILD_UNITTESTS=TRUE
EXT_DEBUG_FLAGS += -DBUILD_EXTENSION_UNIT_TESTS=ON -DBUILD_UNITTESTS=TRUE

# `make test` runs both suites (overrides extension-ci-tools' generic runner).
.PHONY: test_release_internal test_debug_internal test_reldebug_internal
test_release_internal:
	$(MAKE) test-sql-release
	$(MAKE) test-unit-release

test_debug_internal:
	$(MAKE) test-sql-debug
	$(MAKE) test-unit-debug

test_reldebug_internal:
	$(MAKE) test-sql-reldebug
	$(MAKE) test-unit-reldebug

# SQL tests via DuckDB's own unittest binary.
.PHONY: test-sql test-sql-release test-sql-debug test-sql-reldebug
test-sql: test-sql-release
test-sql-release:
	./build/release/test/unittest "[graphar]"
test-sql-debug:
	./build/debug/test/unittest "[graphar]"
test-sql-reldebug:
	./build/reldebug/test/unittest "[graphar]"

# C++ unit tests of the extension.
.PHONY: test-unit test-unit-release test-unit-debug test-unit-reldebug
test-unit: test-unit-release
test-unit-release:
	./build/release/extension/duckdb_graphar/test/cpp/unittest_graphar
	./build/release/extension/duckdb_graphar/test/cpp/analytics/unittest_product_usage_analytics
test-unit-debug:
	./build/debug/extension/duckdb_graphar/test/cpp/unittest_graphar
	./build/debug/extension/duckdb_graphar/test/cpp/analytics/unittest_product_usage_analytics
test-unit-reldebug:
	./build/reldebug/extension/duckdb_graphar/test/cpp/unittest_graphar
	./build/reldebug/extension/duckdb_graphar/test/cpp/analytics/unittest_product_usage_analytics

$(BOOST_INSTALLED):
	@echo "Build and install Boost $(BOOST_VERSION)"
	rm -rf $(BOOST_DIR)
	mkdir -p $(BOOST_SRC_DIR)
	curl --fail --location --retry 3 --output $(BOOST_ARCHIVE) $(BOOST_URL)
	tar -xzf $(BOOST_ARCHIVE) -C $(BOOST_SRC_DIR) --strip-components=1
	cd $(BOOST_SRC_DIR) && ./bootstrap.sh \
		--prefix=$(BOOST_INSTALL_DIR) \
		--with-libraries=json,thread,random,log,filesystem
	cd $(BOOST_SRC_DIR) && ./b2 \
		-j$(MAKE_JOBS) \
		link=static \
		cxxflags=-fPIC \
		install
	@test -f $(BOOST_CMAKE_DIR)/BoostConfig.cmake
	@touch $(BOOST_INSTALLED)

$(ARROW_CLONED):
	@echo "Clone Apache Arrow"
	rm -rf $(ARROW_SRC_DIR)
	git clone --branch apache-arrow-$(ARROW_VERSION) ${ARROW_REP} $(ARROW_SRC_DIR)
	@touch $(ARROW_CLONED)

$(ARROW_BUILT): $(ARROW_CLONED) $(BOOST_INSTALLED)
	@echo "Build Apache Arrow"
	rm -rf $(ARROW_BUILD_DIR)
	mkdir -p $(ARROW_BUILD_DIR)
	cd $(ARROW_BUILD_DIR) && \
	cmake .. \
		-DCMAKE_POSITION_INDEPENDENT_CODE=ON \
		-DCMAKE_CXX_FLAGS=-I$(BOOST_INSTALL_DIR)/include \
		-DBoost_SOURCE=SYSTEM \
		-DBoost_ROOT=$(BOOST_INSTALL_DIR) \
		-DBoost_DIR=$(BOOST_CMAKE_DIR) \
		-DBoost_NO_SYSTEM_PATHS=ON \
		-DARROW_BUILD_TESTS=OFF \
		-DARROW_BUILD_BENCHMARKS=OFF \
		-DARROW_BUILD_EXAMPLES=OFF \
		-DARROW_RPATH_ORIGIN=ON \
		-DARROW_COMPUTE=ON \
		-DARROW_CSV=ON \
		-DARROW_DATASET=ON \
		-DARROW_FILESYSTEM=ON \
		-DARROW_JSON=ON \
		-DARROW_ORC=ON \
		-DARROW_PARQUET=ON \
		-DARROW_S3=ON \
		-DARROW_WITH_BROTLI=OFF \
		-DARROW_WITH_BZ2=OFF \
		-DARROW_WITH_LZ4=OFF \
		-DARROW_WITH_SNAPPY=ON \
		-DARROW_WITH_ZLIB=ON \
		-DARROW_WITH_ZSTD=ON \
		-DARROW_GANDIVA=OFF \
		-DARROW_TESTING=OFF \
		-DCMAKE_INSTALL_PREFIX=$(ARROW_INSTALL_DIR) \
		-DARROW_DEPENDENCY_SOURCE=BUNDLED \
		-DARROW_DEPENDENCY_USE_SHARED=OFF \
		-G Ninja
	@grep -F "Boost_DIR" $(ARROW_BUILD_DIR)/CMakeCache.txt | grep -F "=$(BOOST_CMAKE_DIR)" >/dev/null
	@touch $(ARROW_BUILT)

$(ARROW_INSTALLED): $(ARROW_BUILT)
	@echo "Install Apache Arrow"
	rm -rf $(ARROW_INSTALL_DIR)
	cd $(ARROW_BUILD_DIR) && \
	ninja -j$(shell getconf _NPROCESSORS_ONLN) && \
	ninja install
	@touch $(ARROW_INSTALLED)

$(GRAPHAR_CLONED): $(ARROW_INSTALLED)
	@echo "Clone Apache GraphAr"
	rm -rf $(GRAPHAR_DIR)
	mkdir -p $(GRAPHAR_DIR)
	git clone $(GRAPHAR_REP) $(GRAPHAR_SRC_DIR)
	git -C $(GRAPHAR_SRC_DIR) checkout $(GRAPHAR_COMMIT)
	@touch $(GRAPHAR_CLONED)

$(GRAPHAR_BUILT): $(GRAPHAR_CLONED)
	@echo "Build Apache GraphAr"
	rm -rf $(GRAPHAR_BUILD_DIR)
	mkdir -p $(GRAPHAR_BUILD_DIR)
	cd $(GRAPHAR_BUILD_DIR) && \
	cmake .. \
		-DCMAKE_BUILD_TYPE=Release \
		-DGRAPHAR_BUILD_STATIC=ON \
		-DUSE_STATIC_ARROW=ON \
		-DCMAKE_PREFIX_PATH=$(ARROW_INSTALL_DIR) \
		-DCMAKE_INSTALL_PREFIX=$(GRAPHAR_INSTALL_DIR) \
		-DCMAKE_CXX_FLAGS=-fPIC \
		-DCMAKE_C_FLAGS=-fPIC \
		-G Ninja
	@touch $(GRAPHAR_BUILT)

$(GRAPHAR_INSTALLED): $(GRAPHAR_BUILT)
	@echo "Install Apache GraphAr"
	rm -rf $(GRAPHAR_INSTALL_DIR)
	cd $(GRAPHAR_BUILD_DIR) && \
	ninja -j$(shell getconf _NPROCESSORS_ONLN) && \
	ninja install
	@touch $(GRAPHAR_INSTALLED)

$(THIRD_PARTY_CMAKE): $(BOOST_INSTALLED) $(ARROW_INSTALLED) $(GRAPHAR_INSTALLED)
	@echo 'set(Boost_ROOT "$(BOOST_INSTALL_DIR)" CACHE PATH "Path to Boost")' > $(THIRD_PARTY_CMAKE)
	@echo 'set(Boost_DIR "$(BOOST_CMAKE_DIR)" CACHE PATH "Path to Boost CMake package")' >> $(THIRD_PARTY_CMAKE)
	@echo 'set(ARROW_ROOT "$(ARROW_ROOT)" CACHE PATH "Path to Arrow")' >> $(THIRD_PARTY_CMAKE)
	@echo 'set(GRAPHAR_ROOT "$(GRAPHAR_ROOT)" CACHE PATH "Path to GraphAr")' >> $(THIRD_PARTY_CMAKE)

configure_ci: $(THIRD_PARTY_CMAKE)

# Override extension-ci-tools' release/debug. We configure only once (guarded
# by the CMakeCache) to avoid a forced full recompile, but always re-run cmake
# on an existing build so CMake performs an incremental configure and refreshes
# the ever-changing EXTENSION_GIT_COMMIT_HASH / EXTENSION_BUILD_TIMESTAMP when
# new sources are picked up.
.PHONY: release debug
release: $(THIRD_PARTY_CMAKE) $(EXTENSION_CONFIG_STEP)
	mkdir -p build/release
	@test -f build/release/CMakeCache.txt || cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=20 -S $(DUCKDB_SRCDIR) -B build/release
	cmake -S $(DUCKDB_SRCDIR) -B build/release -DCMAKE_CXX_STANDARD=20
	cmake --build build/release --config Release

debug: $(THIRD_PARTY_CMAKE) $(EXTENSION_CONFIG_STEP)
	mkdir -p build/debug
	@test -f build/debug/CMakeCache.txt || cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_STANDARD=20 -S $(DUCKDB_SRCDIR) -B build/debug
	cmake -S $(DUCKDB_SRCDIR) -B build/debug -DCMAKE_CXX_STANDARD=20
	cmake --build build/debug --config Debug
