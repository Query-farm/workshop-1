#define DUCKDB_EXTENSION_MAIN

#include "workshop_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

void EasterScalarFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &year_vector = args.data[0];
	UnaryExecutor::Execute<int64_t, date_t>(year_vector, result, args.size(), [&](int64_t year) {
		if (year <= 1582 || year >= 4099) {
			throw InvalidInputException("Year must be greater than 1582 and less than 4099");
		}
		int a = year % 19;
		int b = year / 100;
		int c = year % 100;
		int d = b / 4;
		int e = b % 4;
		int f = (b + 8) / 25;
		int g = (b - f + 1) / 3;
		int h = (19 * a + b - d - g + 15) % 30;
		int i = c / 4;
		int k = c % 4;
		int l = (32 + 2 * e + 2 * i - h - k) % 7;
		int m = (a + 11 * h + 22 * l) / 451;

		int month = (h + l - 7 * m + 114) / 31; // 3=March, 4=April
		int day = ((h + l - 7 * m + 114) % 31) + 1;

		return duckdb::Date::FromDate(year, month, day);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction("easter", {LogicalType::BIGINT}, LogicalType::DATE, EasterScalarFunc));
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
