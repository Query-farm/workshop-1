#define DUCKDB_EXTENSION_MAIN

#include "workshop_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Build your extension here.
}

void WorkshopExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string WorkshopExtension::Name() {
	return "workshop";
}

std::string WorkshopExtension::Version() const {
	return "2025012201";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(workshop, loader) {
	duckdb::LoadInternal(loader);
}
}
