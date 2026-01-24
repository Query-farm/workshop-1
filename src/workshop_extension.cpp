#define DUCKDB_EXTENSION_MAIN

#include "workshop_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
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

struct IncrementalSequenceBindData : public FunctionData {
	const int64_t start_value, end_value;

	IncrementalSequenceBindData(int64_t start_value_p, int64_t end_value_p)
	    : start_value(start_value_p), end_value(end_value_p) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IncrementalSequenceBindData>(start_value, end_value);
	}
	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<IncrementalSequenceBindData>();
		return start_value == other.start_value && end_value == other.end_value;
	}
};

unique_ptr<FunctionData> IncrementalSequenceFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	int64_t start_value = input.inputs[0].GetValue<int64_t>();
	int64_t end_value = input.inputs[1].GetValue<int64_t>();

	if (end_value < start_value) {
		throw InvalidInputException("End value must be greater than or equal to start value");
	}

	names.push_back("sequence_value");
	return_types.push_back(LogicalType::BIGINT);
	return make_uniq<IncrementalSequenceBindData>(start_value, end_value);
}

struct IncrementalSequenceGlobalState : public GlobalTableFunctionState {
	int64_t current_value;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<GlobalTableFunctionState> IncrementalSequenceGlobalInit(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IncrementalSequenceBindData>();
	auto state = make_uniq<IncrementalSequenceGlobalState>();
	state->current_value = bind_data.start_value;
	return state;
}

void IncrementalSequenceFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IncrementalSequenceBindData>();
	auto &global_state = data.global_state->Cast<IncrementalSequenceGlobalState>();

	if (global_state.current_value > bind_data.end_value) {
		output.SetCardinality(0);
		return;
	}

	int64_t remaining = bind_data.end_value - global_state.current_value;
	idx_t row_count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
	output.SetCardinality(row_count);
	output.data[0].Sequence(global_state.current_value, 1, row_count);
	global_state.current_value += row_count;
}

static void LoadInternal(ExtensionLoader &loader) {
	ScalarFunction easter_func("easter", {LogicalType::BIGINT}, LogicalType::DATE, EasterScalarFunc);

	CreateScalarFunctionInfo easter_info(easter_func);
	FunctionDescription easter_desc;
	easter_desc.description =
	    "Calculates the date of Easter Sunday for a given year using the Anonymous Gregorian algorithm. "
	    "Valid for years between 1583 and 4098 (Gregorian calendar era).";
	easter_desc.examples = {"easter(2025)"};
	easter_desc.categories = {"date"};
	easter_desc.parameter_names = {"year"};
	easter_info.descriptions.push_back(easter_desc);
	loader.RegisterFunction(easter_info);

	TableFunction incremental_sequence_func("incremental_sequence", {LogicalType::BIGINT, LogicalType::BIGINT},
	                                        IncrementalSequenceFunc, IncrementalSequenceFuncBind,
	                                        IncrementalSequenceGlobalInit);
	loader.RegisterFunction(incremental_sequence_func);
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
