# Workshop DuckDB Extension

C++ extension for DuckDB v1.4.3.

## Build Commands

```bash
make release          # Release build (default)
make debug            # Debug build
GEN=ninja make        # Faster builds with Ninja (requires ccache)
make clean            # Clean build artifacts
```

## Test Commands

```bash
make test             # Run tests (release)
make test_debug       # Run tests (debug)
```

Prefer to build the debug version because it adds additional tests.

## Project Structure

```
src/
  include/workshop_extension.hpp   # Extension class definition
  workshop_extension.cpp           # Main implementation
test/sql/workshop.test             # SQL logic tests
build/
  debug/duckdb                     # Debug shell with extension
  release/duckdb                   # Release shell with extension
```


## Adding Functionality

Edit `src/workshop_extension.cpp` in the `LoadInternal()` function to register new scalar functions, table functions, or other DuckDB features.

The extension class in `src/include/workshop_extension.hpp` defines:
- `Load()` - Called when extension loads
- `Name()` - Returns "workshop"
- `Version()` - Returns version string

## Test Format

Tests use DuckDB's SQLLogicTest format in `test/sql/workshop.test`:

```
require workshop

query I
SELECT workshop('test');
----
Workshop test 🐥
```

## Dependencies

Managed via VCPKG (`vcpkg.json`). Currently includes OpenSSL.
