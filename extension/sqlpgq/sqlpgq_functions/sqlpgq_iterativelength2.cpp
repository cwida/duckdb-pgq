#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_common.hpp"
#include "sqlpgq_functions.hpp"

namespace duckdb {

static bool IterativeLength2(int64_t v_size, int64_t *V, vector<int64_t> &E, vector<std::bitset<LANE_LIMIT>> &seen,
                             vector<std::bitset<LANE_LIMIT>> &visit, vector<std::bitset<LANE_LIMIT>> &next) {
	std::bitset<LANE_LIMIT> change;
	for (auto v = 0; v < v_size; v++) {
		seen[v] |= visit[v];
		next[v] = 0;
	}
	for (auto v = 0; v < v_size; v++) {
		if (visit[v].any()) {
			for (auto e = V[v]; e < V[v + 1]; e++) {
				auto n = E[e];
				auto unseen = visit[v] & ~seen[n];
				next[n] |= unseen;
				change |= unseen;
			}
		}
	}
	return change.any();
}

static void IterativeLength2Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;

	// get csr info (TODO: do not store in context -- make global map in module that is indexed by id+&context)
	int32_t id = args.data[0].GetValue(0).GetValue<int32_t>();
	D_ASSERT(info.context.client_data->csr_list[id]);
	int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();
	int64_t *v = (int64_t *)info.context.client_data->csr_list[id]->v;
	vector<int64_t> &e = info.context.client_data->csr_list[id]->e;

	// get src and dst vectors for searches
	auto &src = args.data[2];
	auto &dst = args.data[3];
	UnifiedVectorFormat vdata_src;
	UnifiedVectorFormat vdata_dst;
	src.ToUnifiedFormat(args.size(), vdata_src);
	dst.ToUnifiedFormat(args.size(), vdata_dst);
	auto src_data = (int64_t *)vdata_src.data;
	auto dst_data = (int64_t *)vdata_dst.data;

	// create result vector
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int64_t>(result);

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
					result_data[search_num] = (uint64_t)-1; // no path
				} else if (src_data[src_pos] == dst_data[dst_pos]) {
					result_data[search_num] = (uint64_t)0; // path of length 0 does not require a search
				} else {
					visit1[src_data[src_pos]][lane] = 1;
					lane_to_num[lane] = search_num; // active lane
					active++;
					break;
				}
			}
		}

		// make passes while a lane is still active
		for (int64_t iter = 1; active; iter++) {
			//            std::cout << "Single direction iteration: " << iter << std::endl;

			if (!IterativeLength2(v_size, v, e, seen, (iter & 1) ? visit1 : visit2, (iter & 1) ? visit2 : visit1)) {
				break;
			}
			// detect lanes that finished
			for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
				int64_t search_num = lane_to_num[lane];
				if (search_num >= 0) { // active lane
					int64_t dst_pos = vdata_dst.sel->get_index(search_num);
					if ((iter & 1) ? visit2[dst_data[dst_pos]][lane] : visit1[dst_data[dst_pos]][lane]) {
						result_data[search_num] = iter; /* found at iter => iter = path length */
						lane_to_num[lane] = -1;         // mark inactive
						active--;
					}
				}
			}
		}
		// no changes anymore: any still active searches have no path
		for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
			int64_t search_num = lane_to_num[lane];
			if (search_num >= 0) {                     // active lane
				result_validity.SetInvalid(search_num);
				result_data[search_num] = (int64_t)-1; /* no path */
				lane_to_num[lane] = -1;                // mark inactive
			}
		}
	}
}

CreateScalarFunctionInfo SQLPGQFunctions::GetIterativeLength2Function() {
	auto fun = ScalarFunction("iterativelength2",
	                          {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                          LogicalType::BIGINT, IterativeLength2Function, IterativeLengthFunctionData::IterativeLengthBind);
	return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb