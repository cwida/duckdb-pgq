#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_functions.hpp"
#include "sqlpgq_common.hpp"

#include <iostream>

namespace duckdb {

static bool IterativeLengthBidirectional2(int64_t v_size, int64_t *V, vector<int64_t> &E, vector<std::bitset<LANE_LIMIT>> &seen,
                            vector<std::bitset<LANE_LIMIT>> &visit, vector<std::bitset<LANE_LIMIT>> &next) {
	bool change = false;
	for (auto v = 0; v < v_size; v++) {
		next[v] = 0;
	}
	for (auto v = 0; v < v_size; v++) {
		if (visit[v].any()) {
			for (auto e = V[v]; e < V[v + 1]; e++) {
				auto n = E[e];
				next[n] = next[n] | visit[v];
			}
		}
	}
	for (auto v = 0; v < v_size; v++) {
		next[v] = next[v] & ~seen[v];
		seen[v] = seen[v] | next[v];
		change |= next[v].any();
	}
	return change;
}

static std::bitset<LANE_LIMIT>
InterSectFronteers2(int64_t v_size, vector<std::bitset<LANE_LIMIT>> &src_seen, vector<std::bitset<LANE_LIMIT>> &dst_seen) {
    std::bitset<LANE_LIMIT> result;
    for (auto v = 0; v < v_size; v++) {
        result |= src_seen[v] & dst_seen[v];
    }
    return result;
}

static void IterativeLengthBidirectionalLaneReuseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;

	// get csr info (TODO: do not store in context -- make global map in module that is indexed by id+&context)
	D_ASSERT(info.context.client_data->csr_list[info.csr_id]);
	int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();
	int64_t *v = (int64_t *)info.context.client_data->csr_list[info.csr_id]->v;
	vector<int64_t> &e = info.context.client_data->csr_list[info.csr_id]->e;

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
    ValidityMask &result_validity = FlatVector::Validity(result);
    auto result_data = FlatVector::GetData<int64_t>(result);

	// create temp SIMD arrays
	vector<std::bitset<LANE_LIMIT>> src_seen(v_size);
	vector<std::bitset<LANE_LIMIT>> src_visit1(v_size);
	vector<std::bitset<LANE_LIMIT>> src_visit2(v_size);
	vector<std::bitset<LANE_LIMIT>> dst_seen(v_size);
    vector<std::bitset<LANE_LIMIT>> dst_visit1(v_size);
    vector<std::bitset<LANE_LIMIT>> dst_visit2(v_size);

	// maps lane to search number
	short lane_to_num[LANE_LIMIT];
	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		lane_to_num[lane] = -1; // inactive
	}

	int16_t lane_to_iter[LANE_LIMIT];
	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		lane_to_iter[lane] = 0;
	}

	idx_t started_searches = 0;
	while (started_searches < args.size()) {

		// empty visit vectors
		for (auto i = 0; i < v_size; i++) {
			src_seen[i] = 0;
            dst_seen[i] = 0;
			src_visit1[i] = 0;
            dst_visit1[i] = 0;
		}

		// add search jobs to free lanes
		uint64_t active = 0;
		for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
            lane_to_num[lane] = -1;
			lane_to_iter[lane] = 0;
			while (started_searches < args.size()) {
				int64_t search_num = started_searches++;
				int64_t src_pos = vdata_src.sel->get_index(search_num);
                int64_t dst_pos = vdata_dst.sel->get_index(search_num);
                if (!vdata_src.validity.RowIsValid(src_pos)) {
                    result_validity.SetInvalid(search_num);
                    result_data[search_num] = (uint64_t) - 1; // no path
                } else if (src_data[src_pos] == dst_data[dst_pos]) {
                    result_data[search_num] = (uint64_t) 0; // path of length 0 does not require a search
				} else {
					auto src_value = src_data[src_pos];
					auto dst_value = dst_data[dst_pos];

                    src_visit1[src_value][lane] = true;
                    dst_visit1[dst_value][lane] = true;
                    src_seen[src_value][lane] = true;
                    dst_seen[dst_value][lane] = true;

                    lane_to_num[lane] = search_num; // active lane
					active++;
					break;
				}
			}
		}

		// make passes while a lane is still active
		for (int64_t iter = 0; active; iter++) {
			// refill empty lanes
			for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
				if (lane_to_num[lane] != -1) {
					continue;
				}
				while (started_searches < args.size()) {
					int64_t search_num = started_searches++;
					int64_t src_pos = vdata_src.sel->get_index(search_num);
					int64_t dst_pos = vdata_dst.sel->get_index(search_num);
					if (!vdata_src.validity.RowIsValid(src_pos)) {
						result_validity.SetInvalid(search_num);
						result_data[search_num] = (uint64_t)-1; /* no path */
					} else if (src_data[src_pos] == dst_data[dst_pos]) {
						result_data[search_num] = (uint64_t)0; // path of length 0 does not require a search
					} else {
						// empty visit vectors
						for (auto i = 0; i < v_size; i++) {
							src_seen[i][lane] = false;
							src_visit1[i][lane] = false;
							src_visit2[i][lane] = false;
							dst_seen[i][lane] = false;
							dst_visit1[i][lane] = false;
							dst_visit2[i][lane] = false;
						}
						auto src_value = src_data[src_pos];
						auto dst_value = dst_data[dst_pos];

						src_seen[src_value][lane] = true;
						dst_seen[dst_value][lane] = true;

						(iter&2)?(iter&1)?dst_visit2[dst_value][lane] = src_visit1[src_value][lane] = true : src_visit2[src_value][lane] = dst_visit2[dst_value][lane] = true
								:(iter&1)?dst_visit1[dst_value][lane] = src_visit2[src_value][lane] = true : src_visit1[src_value][lane] = dst_visit1[dst_value][lane] = true;

						lane_to_num[lane] = search_num; // active lane
						lane_to_iter[lane] = iter;
						active++;
						break;
					}
				}
			}

			if (!IterativeLengthBidirectional2(v_size, v, e,
                                              (iter&1)?dst_seen:src_seen,
                                              (iter&2)?(iter&1)?dst_visit2:src_visit2:(iter&1)?dst_visit1:src_visit1,
                                              (iter&2)?(iter&1)?dst_visit1:src_visit1:(iter&1)?dst_visit2:src_visit2)) {
				break;
			}

            std::bitset<LANE_LIMIT> done = InterSectFronteers2(v_size, src_seen, dst_seen);
            // detect lanes that finished
			for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
                if (done[lane]) {
                    int64_t search_num = lane_to_num[lane];
                    if (search_num >= 0) {
                        result_data[search_num] = iter - lane_to_iter[lane] + 1; /* found at iter => iter = path length */
                        lane_to_num[lane] = -1;         // mark inactive
                        active--;
                    }
				}
			}
		}
		// no changes anymore: any still active searches have no path
		for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
			int64_t search_num = lane_to_num[lane];
			if (search_num >= 0) {
                result_validity.SetInvalid(search_num);
				result_data[search_num] = (int64_t)-1; /* no path */
				lane_to_num[lane] = -1;                // mark inactive
			}
		}
	}
}

CreateScalarFunctionInfo SQLPGQFunctions::GetIterativeLengthBidirectionalLaneReuseFunction() {
	auto fun = ScalarFunction("iterativelengthbidirectional_lanereuse",
	                          {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                          LogicalType::BIGINT, IterativeLengthBidirectionalLaneReuseFunction, IterativeLengthFunctionData::IterativeLengthBind);
	return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
