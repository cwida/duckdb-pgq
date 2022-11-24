#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_common.hpp"
#include "sqlpgq_functions.hpp"

#include <iostream>
#include <fstream>

namespace duckdb {

static bool IterativeLength(std::ofstream &file_stream, int64_t v_size, int64_t *v, vector<int64_t> &e, short lane_to_num[], vector<std::bitset<LANE_LIMIT>> &seen,
                            vector<std::bitset<LANE_LIMIT>> &visit, vector<std::bitset<LANE_LIMIT>> &next) {
	bool change = false;
	for (auto i = 0; i < v_size; i++) {
		next[i] = 0;
	}

	for (auto i = 0; i < v_size; i++) {
		if (visit[i].any()) {
			for (auto offset = v[i]; offset < v[i + 1]; offset++) {
				auto n = e[offset];
				next[n] = next[n] | visit[i];
			}
		}
	}


	for (auto i = 0; i < v_size; i++) {
		next[i] = next[i] & ~seen[i];
		seen[i] = seen[i] | next[i];
		change |= next[i].any();
	}

	auto _vertices_with_one_lane = 0;
	auto _vertices_seen = 0;

	for (auto i = 0; i < v_size; i++) {
		if (seen[i].none()) continue;
		_vertices_seen++;
		//		std::cout << "Vertex: " << i << " seen: " << seen[i].count() << " next: " << next[i].count() << std::endl;
	}

	for (auto i = 0; i < v_size; i++) {
		if (next[i].none()) continue;
		_vertices_with_one_lane++;
//		std::cout << "Vertex: " << i << " seen: " << seen[i].count() << " next: " << next[i].count() << std::endl;
	}

	file_stream << "Vertices with at least one lane active next iteration: " << _vertices_with_one_lane << std::endl;
	file_stream << "That is " << ((float)_vertices_with_one_lane/(float)v_size) * 100 << "% of all vertices" << std::endl;

	file_stream << "Vertices that have been seen by at least one lane so far: " << _vertices_seen << std::endl;
	file_stream << "That is " << ((float)_vertices_seen/(float)v_size) * 100 << "% of all vertices" << std::endl;

	file_stream << "!\n";


	std::vector<int> _counter(LANE_LIMIT, 0);
	for (auto i = 0; i < v_size; i++) {
		for (auto j = 0; j < LANE_LIMIT; j++) {
			if (next[i][j]) {
				_counter[j]++;
			}
		}
	}

	for (auto i = 0; i < LANE_LIMIT; i++) {
		if (lane_to_num[i] < 0) {
			file_stream << "Lane " << i << " has finished already" << std::endl;
		} else {
			file_stream << "Lane " << i << " has discovered " << _counter[i] << " new node(s) this iteration" << std::endl;
		}
	}

	return change;
}

static void IterativeLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;

	std::ofstream file_stream;
	file_stream.open(info.file_name, std::ios::app);

	file_stream << "[New vector for iterative length with " << args.size() << " searches total]\n";

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

	ValidityMask &result_validity = FlatVector::Validity(result);


	// create result vector
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int64_t>(result);

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
					result_data[search_num] = (uint64_t)-1; /* no path */
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

		file_stream << "=\nStarting new batch of " << active
		          << " searches. Filling up " << ((float)active / (float)LANE_LIMIT)*100 << "% of lanes." << std::endl;

		auto _iterations = 1;
		auto _finished_searches_total = 0;
		// make passes while a lane is still active
		for (int64_t iter = 1; active; iter++) {
			auto _finished_searches_iter = 0;
			file_stream << "+\n[Starting iteration " << iter << "]\n";
			if (!IterativeLength(file_stream, v_size, v, e, lane_to_num, seen, (iter & 1) ? visit1 : visit2, (iter & 1) ? visit2 : visit1)) {
				break;
			}
			// detect lanes that finished
			for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
				int64_t search_num = lane_to_num[lane];
				if (search_num >= 0) { // active lane
					int64_t dst_pos = vdata_dst.sel->get_index(search_num);
					if (seen[dst_data[dst_pos]][lane]) {
						file_stream << "Lane: " << lane << " finished after " << iter << " iteration(s)." << std::endl;
						result_data[search_num] = iter; /* found at iter => iter = path length */
						lane_to_num[lane] = -1;         // mark inactive
						active--;
						_finished_searches_total++;
						_finished_searches_iter++;
					}
				}
			}
			_iterations++;
			file_stream << "&\nFinished " << _finished_searches_iter << " searches this iteration (" << iter << ")." << std::endl;
			file_stream << "*\nFinished " << _finished_searches_total << " searches so far." << std::endl;
		}
		// no changes anymore: any still active searches have no path
		file_stream << "[Following are lanes with no path]\n";
		for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
			int64_t search_num = lane_to_num[lane];
			if (search_num >= 0) { // active lane
				file_stream << "Lane " << lane << " did not find a path after " << _iterations << " iteration(s)." << std::endl;
				result_validity.SetInvalid(search_num);
				result_data[search_num] = (int64_t)-1; /* no path */
				lane_to_num[lane] = -1;                // mark inactive
			}
		}
	}
	file_stream << "#\n";
	file_stream.close();
}

CreateScalarFunctionInfo SQLPGQFunctions::GetIterativeLengthFunction() {
	auto fun = ScalarFunction(
	    "iterativelength", {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::BIGINT, IterativeLengthFunction, IterativeLengthFunctionData::IterativeLengthBind);
	return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
