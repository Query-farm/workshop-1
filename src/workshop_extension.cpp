#define DUCKDB_EXTENSION_MAIN

#include "workshop_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>

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

	names.push_back("value");
	names.push_back("thread_id");
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::UBIGINT);
	return make_uniq<IncrementalSequenceBindData>(start_value, end_value);
}

struct WorkItem {
	int64_t start_value;
	int64_t end_value;
};

struct IncrementalSequenceGlobalState : public GlobalTableFunctionState {
	std::queue<WorkItem> work_queue;
	std::mutex queue_mutex;
	std::atomic<idx_t> total_rows_returned {0};
	const int64_t total_rows;

	explicit IncrementalSequenceGlobalState(int64_t total_rows) : GlobalTableFunctionState(), total_rows(total_rows) {
	}

	bool GetWorkItem(WorkItem &item) {
		std::lock_guard<std::mutex> lock(queue_mutex);
		if (work_queue.empty()) {
			return false;
		}
		item = work_queue.front();
		work_queue.pop();
		return true;
	}

	idx_t MaxThreads() const override {
		return GlobalTableFunctionState::MAX_THREADS;
	}
};

struct IncrementalSequenceLocalState : public LocalTableFunctionState {
	int64_t current_value = 1;
	int64_t end_value = 0;
	bool has_work = false;
	const int64_t thread_id;

	explicit IncrementalSequenceLocalState()
	    : LocalTableFunctionState(), has_work(false),
	      thread_id(std::hash<std::thread::id> {}(std::this_thread::get_id())) {
	}
};

static unique_ptr<LocalTableFunctionState> IncrementalSequenceLocalInit(ExecutionContext &context,
                                                                        TableFunctionInitInput &input,
                                                                        GlobalTableFunctionState *global_state_p) {
	return make_uniq<IncrementalSequenceLocalState>();
}

static unique_ptr<GlobalTableFunctionState> IncrementalSequenceGlobalInit(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IncrementalSequenceBindData>();
	auto state = make_uniq<IncrementalSequenceGlobalState>(bind_data.end_value - bind_data.start_value);

	int64_t num_threads = std::min(state->total_rows, (int64_t)TaskScheduler::GetScheduler(context).NumberOfThreads());
	auto current_start = bind_data.start_value;
	const auto values_per_chunk = state->total_rows / num_threads;
	const auto remainder = state->total_rows % num_threads;

	for (auto i = 0; i < num_threads; i++) {
		auto chunk_size = values_per_chunk + ((i == num_threads - 1) ? remainder : 0);
		state->work_queue.push({current_start, current_start + chunk_size});
		current_start += chunk_size;
	}

	return state;
}

unique_ptr<NodeStatistics> IncrementalSequenceCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<IncrementalSequenceBindData>();

	return make_uniq<NodeStatistics>(static_cast<idx_t>(bind_data.end_value - bind_data.start_value));
}

void IncrementalSequenceFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<IncrementalSequenceGlobalState>();
	auto &local_state = data.local_state->Cast<IncrementalSequenceLocalState>();

	// Get new work if needed (end_value is exclusive)
	if (local_state.current_value >= local_state.end_value) {
		WorkItem item;
		if (!global_state.GetWorkItem(item)) {
			// No more work available
			output.SetCardinality(0);
			return;
		}
		local_state.current_value = item.start_value;
		local_state.end_value = item.end_value;
	}

	idx_t row_count = MinValue<idx_t>(local_state.end_value - local_state.current_value, STANDARD_VECTOR_SIZE);

	output.SetCardinality(row_count);
	output.data[0].Sequence(local_state.current_value, 1, row_count);
	output.data[1].Reference(Value::UBIGINT(local_state.thread_id));
	local_state.current_value += row_count;
	global_state.total_rows_returned.fetch_add(row_count, std::memory_order_relaxed);
}

double IncrementalSequenceProgress(ClientContext &context, const FunctionData *bind_data_p,
                                   const GlobalTableFunctionState *global_state_p) {
	auto &global_state = global_state_p->Cast<IncrementalSequenceGlobalState>();
	if (global_state.total_rows == 0) {
		return 100.0;
	}
	return static_cast<double>(global_state.total_rows_returned.load(std::memory_order_relaxed)) /
	       static_cast<double>(global_state.total_rows) * 100.0;
}

unique_ptr<BaseStatistics> IncrementalSequenceStatistics(ClientContext &context, const FunctionData *bind_data_ptr,
                                                         column_t column_index) {
	auto &bind_data = bind_data_ptr->Cast<IncrementalSequenceBindData>();

	if (column_index == 0) {
		auto r = NumericStats::CreateEmpty(LogicalType::BIGINT);
		NumericStats::SetMin(r, bind_data.start_value);
		NumericStats::SetMax(r, bind_data.end_value);
		r.SetHasNoNull();
		return r.ToUnique();
	} else if (column_index == 1) {
		auto r = NumericStats::CreateEmpty(LogicalType::UBIGINT);
		r.SetHasNoNull();
		r.SetDistinctCount((int64_t)TaskScheduler::GetScheduler(context).NumberOfThreads());
		return r.ToUnique();
	} else {
		throw InvalidInputException("Unknown column requested for statistics");
	}
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
	                                        IncrementalSequenceGlobalInit, IncrementalSequenceLocalInit);
	incremental_sequence_func.cardinality = IncrementalSequenceCardinality;
	incremental_sequence_func.table_scan_progress = IncrementalSequenceProgress;
	incremental_sequence_func.statistics = IncrementalSequenceStatistics;
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
