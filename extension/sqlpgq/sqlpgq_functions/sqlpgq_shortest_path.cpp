#include "duckdb/common/fstream.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/sql_pgq_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_functions.hpp"

#include <iostream>

namespace duckdb {

template <typename T>
std::ostream &operator<<(std::ostream &os, const std::vector<T> &v) {
	for (T const &i : v) {
		os << i << " ";
	}
	return os;
}

struct ShortestPathBindData : public FunctionData {
	ClientContext &context;
	string file_name;

	ShortestPathBindData(ClientContext &context, string &file_name) : context(context), file_name(file_name) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<ShortestPathBindData>(context, file_name);
	}
};

struct AnyShortestPathBindData : public FunctionData {
	ClientContext &context;

	explicit AnyShortestPathBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<AnyShortestPathBindData>(context);
	}
};

struct BfsParent {
	int64_t index;
	bool is_set = false;

	explicit BfsParent(int64_t index) : index(index), is_set(false) {
	}
	BfsParent(int64_t index, bool is_set) : index(index), is_set(is_set) {
	}
};

template <typename T, typename S>
std::unordered_map<int16_t, std::vector<S>> CreateCopy(unordered_map<int16_t, std::vector<T>> const &dict) {
	unordered_map<int16_t, std::vector<S>> new_dict;

	for (auto val : dict) {
		std::vector<S> old_vector = std::vector<S>(val.second.begin(), val.second.end());
		std::replace(old_vector.begin(), old_vector.end(), (S)std::numeric_limits<T>::max(),
		             std::numeric_limits<S>::max());
		new_dict[val.first] = old_vector;
	}
	return new_dict;
}

static int16_t InitialiseBfs(idx_t curr_batch, idx_t size, int64_t *src_data, const SelectionVector *src_sel,
                             const ValidityMask &src_validity, vector<std::bitset<LANE_LIMIT>> &seen,
                             vector<std::bitset<LANE_LIMIT>> &visit, vector<std::bitset<LANE_LIMIT>> &visit_next,
                             unordered_map<int64_t, pair<int16_t, vector<int64_t>>> &lane_map, const uint64_t bfs_depth,
                             unordered_map<int16_t, vector<uint8_t>> &depth_map, int64_t input_size) {
	int16_t lanes = 0;
	int16_t curr_batch_size = 0;

	for (idx_t i = curr_batch; i < size && lanes < LANE_LIMIT; i++) {
		auto src_index = src_sel->get_index(i);

		if (src_validity.RowIsValid(src_index)) {
			int64_t &src_entry = src_data[src_index];
			auto entry = lane_map.find(src_entry);
			if (entry == lane_map.end()) {
				lane_map[src_entry].first = lanes;
				seen[src_entry][lanes] = true;
				visit[src_entry][lanes] = true;
				depth_map[lanes] = vector<uint8_t>(input_size, UINT8_MAX);
				depth_map[lanes][src_entry] = (uint8_t)bfs_depth;
				lanes++;
			}
			lane_map[src_entry].second.push_back(i);
			curr_batch_size++;
		}
	}
	return curr_batch_size;
}

static int16_t InitialiseBfs(idx_t curr_batch, idx_t size, int64_t *src_data, const SelectionVector *src_sel,
                             const ValidityMask &src_validity, vector<std::bitset<LANE_LIMIT>> &seen,
                             vector<std::bitset<LANE_LIMIT>> &visit, vector<std::bitset<LANE_LIMIT>> &visit_next,
                             unordered_map<int64_t, pair<int16_t, vector<int64_t>>> &lane_map, const uint64_t bfs_depth,
                             unordered_map<int16_t, vector<uint8_t>> &depth_map, int64_t input_size,
                             unordered_map<int64_t, vector<int64_t>> &final_path,
                             unordered_map<int64_t, vector<BfsParent>> &intermediate_path) {
	int16_t lanes = 0;
	int16_t curr_batch_size = 0;

	for (idx_t i = curr_batch; i < size && lanes < LANE_LIMIT; i++) {
		auto src_index = src_sel->get_index(i);

		if (src_validity.RowIsValid(src_index)) {
			int64_t &src_entry = src_data[src_index];
			auto entry = lane_map.find(src_entry);
			if (entry == lane_map.end()) {
				lane_map[src_entry].first = lanes;
				seen[src_entry][lanes] = true;
				visit[src_entry][lanes] = true;
				depth_map[lanes] = vector<uint8_t>(input_size, UINT8_MAX);
				depth_map[lanes][src_entry] = (uint8_t)bfs_depth;
				if (final_path.find(src_entry) == final_path.end()) {
					final_path[lanes] = vector<int64_t>(input_size, -1);
					final_path[lanes][src_entry] = -2;
					intermediate_path[lanes] = vector<BfsParent>(input_size, BfsParent(-1));
					intermediate_path[lanes][src_entry] = BfsParent(src_entry, true);
				}
				lanes++;
			}
			lane_map[src_entry].second.push_back(i);
			curr_batch_size++;
		}
	}
	return curr_batch_size;
}

template <typename T>
void UpdateBfsDepth(const vector<std::bitset<LANE_LIMIT>> &visit_next, uint64_t bfs_depth,
                    unordered_map<int16_t, vector<T>> &depth_map) {
	for (uint64_t bfs_index = 0; bfs_index < visit_next.size(); bfs_index++) {
		if (!visit_next[bfs_index].any()) {
			continue;
		}
		for (uint64_t map_index = 0; map_index < visit_next[bfs_index].size(); map_index++) {
			if (visit_next[bfs_index][map_index]) {
				if (depth_map[map_index][bfs_index] != std::numeric_limits<T>::max()) {
					continue;
				}
				depth_map[map_index][bfs_index] = bfs_depth;
			}
		}
	}
}

template <typename T>
static bool BfsWithoutArray(bool exit_early, int32_t id, int64_t input_size, ShortestPathBindData &info,
                            vector<std::bitset<LANE_LIMIT>> &seen, vector<std::bitset<LANE_LIMIT>> &visit,
                            vector<std::bitset<LANE_LIMIT>> &visit_next, uint64_t bfs_depth,
                            unordered_map<int16_t, vector<T>> &depth_map) {
	//! For every vertex check edges and update visit_next
	for (int64_t i = 0; i < input_size; i++) {
		if (!visit[i].any()) {
			continue;
		}

		D_ASSERT(info.context.csr_list[id]);
		for (auto index = (int64_t)info.context.csr_list[id]->v[i];
		     index < (int64_t)info.context.csr_list[id]->v[i + 1]; // v or v_weight depending on what was last called?
		     index++) {
			auto n = info.context.csr_list[id]->e[index];
			visit_next[n] = visit_next[n] | visit[i];
		}
	}

	//! set BFS depth
	UpdateBfsDepth<T>(visit_next, bfs_depth, depth_map);

	//! Update seen and visit_next
	for (int64_t i = 0; i < input_size; i++) {
		if (visit_next[i].none()) {
			continue;
		}
		visit_next[i] = visit_next[i] & ~seen[i];
		seen[i] = seen[i] | visit_next[i];
		if (exit_early && visit_next[i].any()) {
			exit_early = false;
		}
	}
	return exit_early;
}

template <typename T>
static bool BfsWithoutArray(bool exit_early, int32_t id, int64_t input_size, AnyShortestPathBindData &info,
                            vector<std::bitset<LANE_LIMIT>> &seen, vector<std::bitset<LANE_LIMIT>> &visit,
                            vector<std::bitset<LANE_LIMIT>> &visit_next, uint64_t bfs_depth,
                            unordered_map<int16_t, vector<T>> &depth_map,
                            unordered_map<int64_t, vector<int64_t>> &final_path,
                            unordered_map<int64_t, vector<BfsParent>> &intermediate_path) {
	for (int64_t i = 0; i < input_size; i++) {
		if (!visit[i].any()) {
			continue;
		}

		D_ASSERT(info.context.csr_list[id]);
		for (auto index = (int64_t)info.context.csr_list[id]->v[i];
		     index < (int64_t)info.context.csr_list[id]->v[i + 1]; // v or v_weight depending on what was last called?
		     index++) {
			auto n = info.context.csr_list[id]->e[index];
			visit_next[n] = visit_next[n] | visit[i];
			for (uint64_t idx = 0; idx < visit_next[n].size(); idx++) {
				if (intermediate_path.find(idx) != intermediate_path.end()) {
					if (visit_next[n][idx] && !intermediate_path[idx][n].is_set) {
						intermediate_path[idx][n] = BfsParent(i, true);
					}
				}
			}
		}
	}

	for (const auto &element : intermediate_path) {
		for (uint64_t map_index = 0; map_index < intermediate_path[element.first].size(); map_index++) {
			if (final_path[element.first][map_index] == -1 && intermediate_path[element.first][map_index].is_set) {
				final_path[element.first][map_index] = intermediate_path[element.first][map_index].index;
				intermediate_path[element.first][map_index].index = map_index;
			}
		}
	}

	for (uint64_t bfs_index = 0; bfs_index < visit_next.size(); bfs_index++) {
		if (!visit_next[bfs_index].any()) {
			continue;
		}
		for (uint64_t map_index = 0; map_index < visit_next[bfs_index].size(); map_index++) {
			if (visit_next[bfs_index][map_index]) {
				if (depth_map[map_index][bfs_index] != std::numeric_limits<T>::max()) {
					continue;
				}
				depth_map[map_index][bfs_index] = bfs_depth;
			}
		}
	}

	for (int64_t i = 0; i < input_size; i++) {
		if (visit_next[i].none()) {
			continue;
		}
		visit_next[i] = visit_next[i] & ~seen[i];
		seen[i] = seen[i] | visit_next[i];
		if (exit_early && visit_next[i].any()) {
			exit_early = false;
		}
	}
	return exit_early;
}

template <typename T>
static void CreateResultShortestPath(const VectorData &vdata_target, const uint64_t *target_data, uint64_t *result_data,
                                     ValidityMask &validity,
                                     unordered_map<int64_t, pair<int16_t, vector<int64_t>>> &lane_map,
                                     unordered_map<int16_t, vector<T>> &depth_map) {
	for (const auto &iter : lane_map) {
		auto bfs_num = iter.second.first;
		auto pos = iter.second.second;
		for (auto index : pos) {
			auto target_index = vdata_target.sel->get_index(index);
			auto value = (uint64_t)depth_map[bfs_num][target_data[target_index]];
			if (value == std::numeric_limits<T>::max()) {
				validity.SetInvalid(target_index);
			}
			result_data[index] = value;
		}
	}
}

static void ShortestPathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ShortestPathBindData &)*func_expr.bind_info;

	int32_t id = args.data[0].GetValue(0).GetValue<int32_t>();
	bool is_variant = args.data[1].GetValue(0).GetValue<bool>();
	int64_t input_size = args.data[2].GetValue(0).GetValue<int64_t>();

	auto &src = args.data[3];

	VectorData vdata_src, vdata_target;
	src.Orrify(args.size(), vdata_src);

	auto src_data = (int64_t *)vdata_src.data;

	auto &target = args.data[4];
	target.Orrify(args.size(), vdata_target);
	auto target_data = (uint64_t *)vdata_target.data;

	idx_t result_size = 0;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<uint64_t>(result);
	auto &validity = FlatVector::Validity(result);

//	ofstream log_file;
//	Profiler<system_clock> phase_profiler, outer_profiler, init_profiler;
//	log_file.open(info.file_name, std::ios_base::app);
//	log_file << "Thread id: " << std::this_thread::get_id() << endl;
//	log_file << "Args size: " << std::to_string(args.size()) << endl;
//	outer_profiler.Start();

	//	info.context.init_m = true;

	while (result_size < args.size()) {
		vector<std::bitset<LANE_LIMIT>> seen(input_size);
		vector<std::bitset<LANE_LIMIT>> visit(input_size);
		vector<std::bitset<LANE_LIMIT>> visit_next(input_size);
//		init_profiler.Start();
		//! mapping of src_value ->  (bfs_num/lane, vector of indices in src_data)

		unordered_map<int64_t, pair<int16_t, vector<int64_t>>> lane_map;
		unordered_map<int16_t, vector<uint8_t>> depth_map_uint8;
		unordered_map<int16_t, vector<uint16_t>> depth_map_uint16;
		unordered_map<int16_t, vector<uint32_t>> depth_map_uint32;
		unordered_map<int16_t, vector<uint64_t>> depth_map_uint64;
		uint64_t bfs_depth = 0;
		auto curr_batch_size = InitialiseBfs(result_size, args.size(), src_data, vdata_src.sel, vdata_src.validity,
		                                     seen, visit, visit_next, lane_map, bfs_depth, depth_map_uint8, input_size);
//		init_profiler.End();
//		log_file << "Init time: " << std::to_string(init_profiler.Elapsed()) << endl;
		bool exit_early = false;
		while (!exit_early) {
			if (bfs_depth == UINT8_MAX) {
				depth_map_uint16 = CreateCopy<uint8_t, uint16_t>(depth_map_uint8);
			} else if (bfs_depth == UINT16_MAX) {
				depth_map_uint32 = CreateCopy<uint16_t, uint32_t>(depth_map_uint16);
			} else if (bfs_depth == UINT32_MAX) {
				depth_map_uint64 = CreateCopy<uint32_t, uint64_t>(depth_map_uint32);
			}
			bfs_depth++;
			exit_early = true;
//			log_file << "BFS depth: " << std::to_string(bfs_depth) << endl;
//			phase_profiler.Start();

			if (bfs_depth < UINT8_MAX) {
				exit_early = BfsWithoutArray<uint8_t>(exit_early, id, input_size, info, seen, visit, visit_next,
				                                      bfs_depth, depth_map_uint8);
			} else if (bfs_depth >= UINT8_MAX && bfs_depth < UINT16_MAX) {
				exit_early = BfsWithoutArray<uint16_t>(exit_early, id, input_size, info, seen, visit, visit_next,
				                                       bfs_depth, depth_map_uint16);
			} else if (bfs_depth >= UINT16_MAX && bfs_depth < UINT32_MAX) {
				exit_early = BfsWithoutArray<uint32_t>(exit_early, id, input_size, info, seen, visit, visit_next,
				                                       bfs_depth, depth_map_uint32);
			} else if (bfs_depth >= UINT32_MAX && bfs_depth < UINT64_MAX) {
				exit_early = BfsWithoutArray<uint64_t>(exit_early, id, input_size, info, seen, visit, visit_next,
				                                       bfs_depth, depth_map_uint64);
			}
//			phase_profiler.End();
//			log_file << "BFS time: " << std::to_string(phase_profiler.Elapsed()) << endl;

			visit = visit_next;
			for (auto i = 0; i < input_size; i++) {
				visit_next[i] = 0;
			}
		}
		if (bfs_depth < UINT8_MAX) {
			CreateResultShortestPath<uint8_t>(vdata_target, target_data, result_data, validity, lane_map,
			                                  depth_map_uint8);
		} else if (bfs_depth >= UINT8_MAX && bfs_depth < UINT16_MAX) {
			CreateResultShortestPath<uint16_t>(vdata_target, target_data, result_data, validity, lane_map,
			                                   depth_map_uint16);
		} else if (bfs_depth >= UINT16_MAX && bfs_depth < UINT32_MAX) {
			CreateResultShortestPath<uint32_t>(vdata_target, target_data, result_data, validity, lane_map,
			                                   depth_map_uint32);
		} else {
			CreateResultShortestPath<uint64_t>(vdata_target, target_data, result_data, validity, lane_map,
			                                   depth_map_uint64);
		}

		result_size = result_size + curr_batch_size;
//		log_file << "Batch size: " << std::to_string(curr_batch_size) << endl;
//		log_file << "Result size: " << std::to_string(result_size) << endl;
	}
	//	auto end = std::chrono::high_resolution_clock::now();
//	outer_profiler.End();
//	log_file << "Entire program time: " << std::to_string(outer_profiler.Elapsed()) << endl;
//	log_file << "-" << endl;
	//	auto int_s = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

}

static void AnyShortestPathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (AnyShortestPathBindData &)*func_expr.bind_info;

	int32_t id = args.data[0].GetValue(0).GetValue<int32_t>();
	bool is_variant = args.data[1].GetValue(0).GetValue<bool>();
	int64_t input_size = args.data[2].GetValue(0).GetValue<int64_t>();

	auto &src = args.data[3];

	VectorData vdata_src, vdata_target;
	src.Orrify(args.size(), vdata_src);

	auto src_data = (int64_t *)vdata_src.data;

	auto &target = args.data[4];
	target.Orrify(args.size(), vdata_target);
	auto target_data = (int64_t *)vdata_target.data;

	idx_t result_size = 0;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	//	info.context.init_m = true;
	idx_t total_len = 0;

	while (result_size < args.size()) {
		vector<std::bitset<LANE_LIMIT>> seen(input_size);
		vector<std::bitset<LANE_LIMIT>> visit(input_size);
		vector<std::bitset<LANE_LIMIT>> visit_next(input_size);
		//! mapping of src_value ->  (bfs_num/lane, vector of indices in src_data)

		unordered_map<int64_t, pair<int16_t, vector<int64_t>>> lane_map;
		unordered_map<int64_t, std::vector<BfsParent>> intermediate_path;
		unordered_map<int64_t, std::vector<int64_t>> final_path;

		unordered_map<int16_t, vector<uint8_t>> depth_map_uint8;
		unordered_map<int16_t, vector<uint16_t>> depth_map_uint16;
		unordered_map<int16_t, vector<uint32_t>> depth_map_uint32;
		unordered_map<int16_t, vector<uint64_t>> depth_map_uint64;
		uint64_t bfs_depth = 0;
		auto curr_batch_size =
		    InitialiseBfs(result_size, args.size(), src_data, vdata_src.sel, vdata_src.validity, seen, visit,
		                  visit_next, lane_map, bfs_depth, depth_map_uint8, input_size, final_path, intermediate_path);
		bool exit_early = false;
		while (!exit_early) {
			if (bfs_depth == UINT8_MAX) {
				depth_map_uint16 = CreateCopy<uint8_t, uint16_t>(depth_map_uint8);
			} else if (bfs_depth == UINT16_MAX) {
				depth_map_uint32 = CreateCopy<uint16_t, uint32_t>(depth_map_uint16);
			} else if (bfs_depth == UINT32_MAX) {
				depth_map_uint64 = CreateCopy<uint32_t, uint64_t>(depth_map_uint32);
			}
			bfs_depth++;
			exit_early = true;
			if (bfs_depth < UINT8_MAX) {
				exit_early = BfsWithoutArray(exit_early, id, input_size, info, seen, visit, visit_next, bfs_depth,
				                             depth_map_uint8, final_path, intermediate_path);
			} else if (bfs_depth >= UINT8_MAX && bfs_depth < UINT16_MAX) {
				exit_early = BfsWithoutArray(exit_early, id, input_size, info, seen, visit, visit_next, bfs_depth,
				                             depth_map_uint16, final_path, intermediate_path);
			} else if (bfs_depth >= UINT16_MAX && bfs_depth < UINT32_MAX) {
				exit_early = BfsWithoutArray(exit_early, id, input_size, info, seen, visit, visit_next, bfs_depth,
				                             depth_map_uint32, final_path, intermediate_path);
			} else if (bfs_depth >= UINT32_MAX && bfs_depth < UINT64_MAX) {
				exit_early = BfsWithoutArray(exit_early, id, input_size, info, seen, visit, visit_next, bfs_depth,
				                             depth_map_uint64, final_path, intermediate_path);
			}
			visit = visit_next;
			for (auto i = 0; i < input_size; i++) {
				visit_next[i] = 0;
			}
		}
		//! Create result vector here
		for (idx_t i = 0; i < args.size(); i++) {
			auto target_index = vdata_target.sel->get_index(i);
			if (!vdata_target.validity.RowIsValid(target_index)) {
				result_validity.SetInvalid(i);
			}
			auto source_index = vdata_src.sel->get_index(i);
			if (!vdata_src.validity.RowIsValid(source_index)) {
				result_validity.SetInvalid(i);
			}

			const auto &target_entry = target_data[target_index];
			const auto &source_entry = src_data[source_index];
			const auto &bfs_num = lane_map[source_entry].first;
			auto index = target_entry;
			std::vector<int64_t> output_vector;
			if (index == -2) {                                   // TODO TEST THIS
				output_vector.push_back(src_data[target_index]); //! Source == target in this case
				break;
			}
			while (index != -2) { //! -2 is used to signify source of path, -1 is used to signify no parent
				output_vector.push_back(index);
				index = final_path[bfs_num][index];
				if (index == -1) {
					result_validity.SetInvalid(i);
					break;
				}
			}
			std::reverse(output_vector.begin(), output_vector.end());
			auto output = make_unique<Vector>(LogicalType::LIST(LogicalType::INTEGER));
			for (auto val : output_vector) {
				Value value_to_insert = val;
				ListVector::PushBack(*output, value_to_insert);
			}
			result_data[i].length = ListVector::GetListSize(*output);
			result_data[i].offset = total_len;
			total_len += ListVector::GetListSize(*output);
			ListVector::Append(result, ListVector::GetEntry(*output), ListVector::GetListSize(*output));

			//			result_data[i] = output_vector;
		}
		result_size = result_size + curr_batch_size;
	}
	D_ASSERT(ListVector::GetListSize(result) == total_len);
}

static unique_ptr<FunctionData> ShortestPathBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	int32_t id = ExpressionExecutor::EvaluateScalar(*arguments[0]).GetValue<int32_t>();
	if ((uint64_t)id + 1 > context.csr_list.size()) {
		throw ConstraintException("Invalid ID");
	}
	if (!(context.csr_list[id]->initialized_v && context.csr_list[id]->initialized_e)) {
		throw ConstraintException("Need to initialize CSR before doing shortest path");
	}
	string file_name;
	if (arguments.size() == 6) {
		file_name = ExpressionExecutor::EvaluateScalar(*arguments[5]).GetValue<string>();
	} else {
		file_name = "timings-test.txt";
	}

	return make_unique<ShortestPathBindData>(context, file_name);
}

static unique_ptr<FunctionData> AnyShortestPathBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {

	return make_unique<AnyShortestPathBindData>(context);
}

CreateScalarFunctionInfo SQLPGQFunctions::GetShortestPathFunction() {
	auto fun = ScalarFunction(
	    "shortest_path",
	    {LogicalType::INTEGER, LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::UBIGINT, ShortestPathFunction, false, ShortestPathBind);
	return CreateScalarFunctionInfo(fun);
}

CreateScalarFunctionInfo SQLPGQFunctions::GetAnyShortestPathFunction() {
	//! TODO Figure out if type can be any or should just be INTEGER
	auto fun = ScalarFunction(
	    "any_shortest_path",
	    {LogicalType::INTEGER, LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::LIST(LogicalType::INTEGER), AnyShortestPathFunction, false, AnyShortestPathBind);
	return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb