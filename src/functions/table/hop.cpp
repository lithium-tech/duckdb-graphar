#include "functions/table/hop.hpp"

#include "utils/func.hpp"
#include "utils/benchmark.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/function/table_function.hpp>
#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/main/extension_util.hpp>
#include <duckdb/common/vector_size.hpp>

#include <graphar/api/high_level_reader.h>

#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> TwoHop::Bind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	bool time_logging = GraphArSettings::is_time_logging(context);

	ScopedTimer t("Bind");

	DUCKDB_GRAPHAR_LOG_TRACE("TwoHop::Bind");

	const auto file_path = StringValue::Get(input.inputs[0]);
	const int64_t vid = IntegerValue::Get(input.named_parameters.at("vid"));

	DUCKDB_GRAPHAR_LOG_DEBUG("Load Edge Info");

	auto yaml_content = GetYamlContent(file_path);
	auto edge_info = graphar::EdgeInfo::Load(yaml_content).value();
	if (!edge_info) {
		throw BinderException("No found edge this type");
	}

	DUCKDB_GRAPHAR_LOG_DEBUG("Create BindData");

	const std::string prefix = GetDirectory(file_path);
	auto bind_data = make_uniq<TwoHopBindData>(edge_info, prefix, vid);

	DUCKDB_GRAPHAR_LOG_DEBUG("Set types and names");

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("_graphArSrcIndex");
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("_graphArDstIndex");

	DUCKDB_GRAPHAR_LOG_DEBUG("Bind finish");
	if (time_logging) {
		t.print();
	}

	return std::move(bind_data);
}
//-------------------------------------------------------------------
// State Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> TwoHopGlobalTableFunctionState::Init(ClientContext &context, TableFunctionInitInput &input) {
	auto bind_data = input.bind_data->Cast<TwoHopBindData>();

	return make_uniq<TwoHopGlobalTableFunctionState>(context, bind_data);
}
unique_ptr<GlobalTableFunctionState> OneMoreHopGlobalTableFunctionState::Init(ClientContext &context, TableFunctionInitInput &input) {
	auto bind_data = input.bind_data->Cast<TwoHopBindData>();

	return make_uniq<OneMoreHopGlobalTableFunctionState>(context, bind_data);
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
inline void OneHopExecute(TwoHopGlobalState &state, DataChunk &output, const bool time_logging) {
	auto table = state.GetSrcReader().get(STANDARD_VECTOR_SIZE);

	output.SetCapacity(table->num_rows());
	output.SetCardinality(table->num_rows());

	int num_columns = table->num_columns();


	DUCKDB_GRAPHAR_LOG_DEBUG("OneHopExecute::iterations " + std::to_string(num_columns));

	for (int col_i = 0; col_i < num_columns; ++col_i) {
		auto column = table->column(col_i);
		int64_t row_offset = 0;
		for (const auto& chunk : column->chunks()) {
			auto int_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
			for (int64_t i = 0; i < int_array->length(); ++i) {
				output.SetValue(col_i, row_offset + i, int_array->Value(i));
				if (state.IsOneHop() && col_i == 1) {
					state.AddHopId(int_array->Value(i));
				}
			}
			row_offset += int_array->length();
		}
	}

	DUCKDB_GRAPHAR_LOG_DEBUG("OneHopExecute::Finish " + std::to_string(table->num_rows()));
}

inline void TwoHop::Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	bool time_logging = GraphArSettings::is_time_logging(context);

	ScopedTimer t("Execute");

	DUCKDB_GRAPHAR_LOG_TRACE("TwoHop::Execute");
	DUCKDB_GRAPHAR_LOG_DEBUG("Cast Global state");

	TwoHopGlobalState &gstate = input.global_state->Cast<TwoHopGlobalTableFunctionState>().GetState();

	if (gstate.GetSrcReader().finish()) {
		DUCKDB_GRAPHAR_LOG_DEBUG("Reader finished");
		return;
	}

	DUCKDB_GRAPHAR_LOG_DEBUG("Begin iteration");
	OneHopExecute(gstate, output, time_logging);

	if (gstate.GetSrcReader().finish()) {
		if (gstate.IsOneHop()) {
			gstate.SetOneHop(false);
		}
		while (gstate.GetHopI() < gstate.GetHopIds().size() && gstate.GetSrcReader().finish()) {
			DUCKDB_GRAPHAR_LOG_DEBUG("Find next hop " + std::to_string(gstate.GetHopIds()[gstate.GetHopI()]));
			gstate.GetSrcReader().find_src(gstate.GetHopIds()[gstate.IncrementHopI()]);
		}
	}

	if (time_logging) {
		t.print();
	}
}

inline int64_t OneMoreHopExecute(OneMoreHopGlobalState &state, DataChunk &output, const bool time_logging) {
	DUCKDB_GRAPHAR_LOG_DEBUG("OneMoreHopExecute::get " + std::to_string(STANDARD_VECTOR_SIZE));
	auto table = state.src_reader.get(STANDARD_VECTOR_SIZE);

	output.SetCapacity(table->num_rows());
	output.SetCardinality(table->num_rows());

	int num_columns = table->num_columns();


	DUCKDB_GRAPHAR_LOG_DEBUG("OneMoreHopExecute::iterations " + std::to_string(num_columns));
	std::vector<bool> valid(table->num_rows(), false);
	int64_t number_valid = 0;

	for (int col_i = num_columns - 1; col_i >= 0; --col_i) {
		auto column = table->column(col_i);
		int64_t row_offset = 0;
		for (const auto& chunk : column->chunks()) {
			auto int_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
			for (int64_t i = 0; i < int_array->length(); ++i) {
				if (state.one_hop) {
					output.SetValue(col_i, row_offset + i, int_array->Value(i));
				} else {
					if (col_i == 1) {
						if (state.hop_ids.find(int_array->Value(i)) != state.hop_ids.end()) {
							valid[row_offset + i] = true;
							output.SetValue(col_i, row_offset + i, int_array->Value(i));
							number_valid++;
						}
					} else {
						if (valid[row_offset + i]) {
							output.SetValue(col_i, row_offset + i, int_array->Value(i));
						}
					}
				}

				if (state.one_hop && col_i == 1) {
					state.hop_ids.insert(int_array->Value(i));
				}

				row_offset += int_array->length();
			}
		}
	}

	output.SetCapacity(number_valid);
	output.SetCardinality(number_valid);

	DUCKDB_GRAPHAR_LOG_DEBUG("OneMoreHopExecute::Finish " + std::to_string(number_valid));
	return number_valid;
}

inline void OneMoreHop::Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	bool time_logging = GraphArSettings::is_time_logging(context);

	ScopedTimer t("Execute");

	DUCKDB_GRAPHAR_LOG_TRACE("OneMoreHop::Execute");
	DUCKDB_GRAPHAR_LOG_DEBUG("Cast Global state");

	OneMoreHopGlobalState &gstate = input.global_state->Cast<OneMoreHopGlobalTableFunctionState>().state;

	if (gstate.src_reader.finish()) {
		DUCKDB_GRAPHAR_LOG_DEBUG("Reader finished");
		return;
	}

	DUCKDB_GRAPHAR_LOG_DEBUG("Begin iteration");
	int64_t row_count;
	bool is_one_hop;
	do {
		row_count = OneMoreHopExecute(gstate, output, time_logging);
		is_one_hop = gstate.one_hop;

		if (gstate.src_reader.finish()) {
			if (gstate.one_hop) {
				gstate.one_hop = false;
			}
			while (gstate.hop_i != gstate.hop_ids.end() && gstate.src_reader.finish()) {
				DUCKDB_GRAPHAR_LOG_DEBUG("Find next hop " + std::to_string(*gstate.hop_i));
				gstate.src_reader.find_src(*gstate.hop_i);
				gstate.hop_i++;
			}
		}
	} while (row_count == 0 && !is_one_hop && !gstate.src_reader.finish());

	if (time_logging) {
		t.print();
	}
}
//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
TableFunction TwoHop::GetFunction() {
	TableFunction read_edges("two_hop", {LogicalType::VARCHAR}, Execute, Bind);
	read_edges.init_global = TwoHopGlobalTableFunctionState::Init;
	read_edges.named_parameters["vid"] = LogicalType::INTEGER;

	//	read_edges.filter_pushdown = true;

	return read_edges;
}

void TwoHop::Register(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, GetFunction());
}

TableFunction OneMoreHop::GetFunction() {
	TableFunction read_edges("one_more_hop", {LogicalType::VARCHAR}, Execute, TwoHop::Bind);
	read_edges.init_global = OneMoreHopGlobalTableFunctionState::Init;
	read_edges.named_parameters["vid"] = LogicalType::INTEGER;

	//	read_edges.filter_pushdown = true;

	return read_edges;
}

void OneMoreHop::Register(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, GetFunction());
}
}
