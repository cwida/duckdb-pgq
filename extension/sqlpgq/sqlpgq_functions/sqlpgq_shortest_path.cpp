#include "duckdb/common/fstream.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_common.hpp"
#include "sqlpgq_functions.hpp"

#include <iostream>

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
	int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();

	int64_t *v = (int64_t *)info.context.client_data->csr_list[id]->v;
	vector<int64_t> &e = info.context.client_data->csr_list[id]->e;
	vector<int64_t> &edge_ids = info.context.client_data->csr_list[id]->edge_ids;

	auto &src = args.data[2];
	auto &target = args.data[3];

	UnifiedVectorFormat vdata_src, vdata_dst;
	src.ToUnifiedFormat(args.size(), vdata_src);
	target.ToUnifiedFormat(args.size(), vdata_dst);

	auto src_data = (int64_t *)vdata_src.data;
	auto dst_data = (int64_t *)vdata_dst.data;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	ValidityMask &result_validity = FlatVector::Validity(result);

	// create temp SIMD arrays
	vector<std::bitset<LANE_LIMIT>> seen(v_size);
	vector<std::bitset<LANE_LIMIT>> visit1(v_size);
	vector<std::bitset<LANE_LIMIT>> visit2(v_size);

	// maps lane to search number
	short lane_to_num[LANE_LIMIT];
	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		lane_to_num[lane] = -1; // inactive
	}
	int64_t total_len = 0;

	idx_t started_searches = 0;
	while (started_searches < args.size()) {

		// empty visit vectors
		for (auto i = 0; i < v_size; i++) {
			seen[i] = 0;
			visit1[i] = 0;
		}

		// add search jobs to free lanes
		uint64_t active = 0;
		for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
			lane_to_num[lane] = -1;
			while (started_searches < args.size()) {
				int64_t search_num = started_searches++;
				int64_t src_pos = vdata_src.sel->get_index(search_num);
				int64_t dst_pos = vdata_dst.sel->get_index(search_num);
				if (!vdata_src.validity.RowIsValid(src_pos)) {
					result_validity.SetInvalid(search_num);
				} else if (src_data[src_pos] == dst_data[dst_pos]) {
					unique_ptr<Vector> output = make_unique<Vector>(LogicalType::LIST(LogicalType::INTEGER));
					ListVector::PushBack(*output, src_data[src_pos]);
					result_data[search_num].length = ListVector::GetListSize(*output);
					result_data[search_num].offset = total_len;
					ListVector::Append(result, ListVector::GetEntry(*output), ListVector::GetListSize(*output));
				} else {
					visit1[src_data[src_pos]][lane] = true;
					lane_to_num[lane] = search_num; // active lane
					active++;
					break;
				}
			}
		}




		// no changes anymore: any still active searches have no path
		for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
			int64_t search_num = lane_to_num[lane];
			if (search_num >= 0) {
				result_validity.SetInvalid(search_num);
				lane_to_num[lane] = -1;                // mark inactive
			}
		}
	}
}

CreateScalarFunctionInfo SQLPGQFunctions::GetShortestPathFunction() {
	auto fun = ScalarFunction("shortestpath",
	                          {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                          LogicalType::LIST(LogicalType::INTEGER), ShortestPathFunction,
	                          IterativeLengthFunctionData::IterativeLengthBind);
	return CreateScalarFunctionInfo(fun);
}

}; // namespace duckdb