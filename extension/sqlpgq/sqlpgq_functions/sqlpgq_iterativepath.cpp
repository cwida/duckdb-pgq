#include "duckdb/common/fstream.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_common.hpp"
#include "sqlpgq_functions.hpp"



namespace duckdb {

struct BfsParent {
	int64_t index;
	bool is_set = false;

	explicit BfsParent(int64_t index) : index(index), is_set(false) {
	}
	BfsParent(int64_t index, bool is_set) : index(index), is_set(is_set) {
	}
};


static void ShortestPathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;

	int32_t id = args.data[0].GetValue(0).GetValue<int32_t>();
	int64_t input_size = args.data[2].GetValue(0).GetValue<int64_t>();

	auto &src = args.data[3];

	UnifiedVectorFormat vdata_src, vdata_target;
	src.ToUnifiedFormat(args.size(), vdata_src);

	auto src_data = (int64_t *)vdata_src.data;

	auto &target = args.data[4];
	target.ToUnifiedFormat(args.size(), vdata_target);
	auto target_data = (int64_t *)vdata_target.data;

	idx_t result_size = 0;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	//	info.context.init_m = true;

//	while (result_size < args.size()) {
//		vector<std::bitset<LANE_LIMIT>> seen(input_size);
//		vector<std::bitset<LANE_LIMIT>> visit(input_size);
//		vector<std::bitset<LANE_LIMIT>> visit_next(input_size);
//		//! mapping of src_value ->  (bfs_num/lane, vector of indices in src_data)
//
//		unordered_map<int64_t, pair<int16_t, vector<int64_t>>> lane_map;
//		unordered_map<int64_t, std::vector<BfsParent>> intermediate_path;
//		unordered_map<int64_t, std::vector<int64_t>> final_path;
//
//		unordered_map<int16_t, vector<uint8_t>> depth_map_uint8;
//		unordered_map<int16_t, vector<uint16_t>> depth_map_uint16;
//		unordered_map<int16_t, vector<uint32_t>> depth_map_uint32;
//		unordered_map<int16_t, vector<uint64_t>> depth_map_uint64;
//		uint64_t bfs_depth = 0;
//		auto curr_batch_size =
//		    InitialiseBfs(result_size, args.size(), src_data, vdata_src.sel, vdata_src.validity, seen, visit,
//		                  visit_next, lane_map, bfs_depth, depth_map_uint8, input_size, final_path, intermediate_path);
//		bool exit_early = false;
//		while (!exit_early) {
//			if (bfs_depth == UINT8_MAX) {
//				depth_map_uint16 = CreateCopy<uint8_t, uint16_t>(depth_map_uint8);
//			} else if (bfs_depth == UINT16_MAX) {
//				depth_map_uint32 = CreateCopy<uint16_t, uint32_t>(depth_map_uint16);
//			} else if (bfs_depth == UINT32_MAX) {
//				depth_map_uint64 = CreateCopy<uint32_t, uint64_t>(depth_map_uint32);
//			}
//			bfs_depth++;
//			exit_early = true;
//			if (bfs_depth < UINT8_MAX) {
//				exit_early = BfsWithoutArray(exit_early, id, input_size, info, seen, visit, visit_next, bfs_depth,
//				                             depth_map_uint8, final_path, intermediate_path);
//			} else if (bfs_depth >= UINT8_MAX && bfs_depth < UINT16_MAX) {
//				exit_early = BfsWithoutArray(exit_early, id, input_size, info, seen, visit, visit_next, bfs_depth,
//				                             depth_map_uint16, final_path, intermediate_path);
//			} else if (bfs_depth >= UINT16_MAX && bfs_depth < UINT32_MAX) {
//				exit_early = BfsWithoutArray(exit_early, id, input_size, info, seen, visit, visit_next, bfs_depth,
//				                             depth_map_uint32, final_path, intermediate_path);
//			} else if (bfs_depth >= UINT32_MAX && bfs_depth < UINT64_MAX) {
//				exit_early = BfsWithoutArray(exit_early, id, input_size, info, seen, visit, visit_next, bfs_depth,
//				                             depth_map_uint64, final_path, intermediate_path);
//			}
//			visit = visit_next;
//			for (auto i = 0; i < input_size; i++) {
//				visit_next[i] = 0;
//			}
//		}
//		//! Create result vector here
//		for (idx_t i = result_size; i < result_size + curr_batch_size; i++) {
//			auto target_index = vdata_target.sel->get_index(i);
//			if (!vdata_target.validity.RowIsValid(target_index)) {
//				result_validity.SetInvalid(i);
//			}
//			auto source_index = vdata_src.sel->get_index(i);
//			if (!vdata_src.validity.RowIsValid(source_index)) {
//				result_validity.SetInvalid(i);
//			}
//
//			const auto &target_entry = target_data[target_index];
//			const auto &source_entry = src_data[source_index];
//			const auto &bfs_num = lane_map[source_entry].first;
//			auto index = target_entry;
//			std::vector<int64_t> output_vector;
//			if (index == -2) {                                   // TODO TEST THIS
//				output_vector.push_back(src_data[target_index]); //! Source == target in this case
//				break;
//			}
//			while (index != -2) { //! -2 is used to signify source of path, -1 is used to signify no parent
//				output_vector.push_back(index);
//				index = final_path[bfs_num][index];
//				if (index == -1) {
//					result_validity.SetInvalid(i);
//					break;
//				}
//			}
//			std::reverse(output_vector.begin(), output_vector.end());
//			auto output = make_unique<Vector>(LogicalType::LIST(LogicalType::INTEGER));
//			for (auto val : output_vector) {
//				Value value_to_insert = val;
//				ListVector::PushBack(*output, value_to_insert);
//			}
//			result_data[i].length = ListVector::GetListSize(*output);
//			result_data[i].offset = total_len;
//			total_len += ListVector::GetListSize(*output);
//			ListVector::Append(result, ListVector::GetEntry(*output), ListVector::GetListSize(*output));
//		}
//		result_size = result_size + curr_batch_size;
//	}
//	D_ASSERT(ListVector::GetListSize(result) == total_len);
}




CreateScalarFunctionInfo SQLPGQFunctions::GetShortestPathFunction() {
	auto fun = ScalarFunction(
	    "shortestpath",
	    {LogicalType::INTEGER, LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::LIST(LogicalType::INTEGER), ShortestPathFunction, IterativeLengthFunctionData::IterativeLengthBind);
	return CreateScalarFunctionInfo(fun);
}

}; // namespace duckdb