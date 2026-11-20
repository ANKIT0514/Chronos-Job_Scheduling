# Phase 1: Project skeleton

## What was built

- `CMakeLists.txt` defines the C++17 `chronos` executable and CTest test target.
- `include/chronos/version.hpp` exposes the project version.
- `src/main.cpp` is a runnable hello-world entry point.
- An initial smoke test verified the version. It was replaced by the domain
  unit tests in phase 2.

## Build and verification

Configure with `cmake -S . -B build -G "MinGW Makefiles"`, build with
`cmake --build build`, run tests with
`cmake --build build --target test`, and launch `build/chronos.exe`.

The skeleton was built with GCC 6.3.0 and passed its CTest smoke test. GCC 6
does not support C++17 inline variables, so the version is a namespace-scope
`constexpr` array with internal linkage.

## Design and concurrency

The executable has no shared mutable state, so it has no synchronization or
concurrency hazards. The initial test is a single process-level smoke test.
This intentionally small boundary keeps the first build independent of
services while verifying the compiler, CMake generator, include path, and
CTest integration.
