#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_functions.hpp"

#include <chrono>
#include <float.h>
#include <iostream>
using namespace std::chrono;

namespace duckdb {

struct CheapestPathBindData : public FunctionData {
	ClientContext &context;
	string file_name;

	CheapestPathBindData(ClientContext &context, string &file_name) : context(context), file_name(file_name) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<CheapestPathBindData>(context, file_name);
	}
};

template <typename T>
static int16_t InitialiseBellmanFord(ClientContext &context, const DataChunk &args, int64_t input_size,
                                     const VectorData &vdata_src, const int64_t *src_data, idx_t result_size,
                                     unordered_map<int64_t, std::vector<bool>> &modified,
                                     unordered_map<int64_t, vector<T>> &dists) {
	for (int64_t i = 0; i < input_size; i++) {
		modified[i] = std::vector<bool>(args.size(), false);
		dists[i] = std::vector<T>(args.size(), std::numeric_limits<T>::max());
	}

	int16_t lanes = 0;
	int16_t curr_batch_size = 0;
	for (idx_t i = result_size; i < args.size() && lanes < context.lane_limit; i++) { // && lanes < LANE_LIMIT
		auto src_index = vdata_src.sel->get_index(i);
		if (vdata_src.validity.RowIsValid(src_index)) {
			const int64_t &src_entry = src_data[src_index];
			modified[src_entry][lanes] = true;
			dists[src_entry][lanes] = 0;
			curr_batch_size++;
			lanes++;
		}
	}
	return curr_batch_size;
}

template <typename T>
void CheckUpdateDistance(int64_t v, int64_t n, T weight, unordered_map<int64_t, std::vector<bool>> &modified,
                         unordered_map<int64_t, vector<T>> &dists, bool &changed) {
	for (uint64_t i = 0; i < modified[v].size(); i++) {
		if (modified[v][i]) {
			auto new_dist = std::min(dists[n][i], dists[v][i] + weight);
			//! If the new weight is shorter than existing
			if (new_dist != dists[n][i]) {
				//! Update the distance
				dists[n][i] = new_dist;
				modified[n][i] = true;
				changed = true;
			}
		}
	}
}
template <typename T>
void TemplatedBellmanFord(CheapestPathBindData &info, DataChunk &args, int64_t input_size, Vector &result,
                          VectorData vdata_src, int64_t *src_data, const VectorData &vdata_target, int64_t *target_data,
                          int32_t id, bool is_double) {
	idx_t result_size = 0;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_validity = FlatVector::Validity(result);
	unordered_map<int64_t, std::vector<bool>> modified;
	unordered_map<int64_t, vector<T>> dists;
	while (result_size < args.size()) {
		int16_t curr_batch_size = 0;
		curr_batch_size =
		    InitialiseBellmanFord<T>(info.context, args, input_size, vdata_src, src_data, result_size, modified, dists);
		bool changed = true;
		while (changed) {
			changed = false;
			//! For every v in the input
			for (int64_t v = 0; v < input_size; v++) {
				//! not modified[v].empty()
				if (!std::all_of(modified[v].begin(), modified[v].end(), [](bool v) { return !v; })) {
					//! Loop through all the n neighbours of v
					if (is_double) {
						for (auto index = (int64_t)info.context.csr_list[id]->v_weight[v];
						     index < (int64_t)info.context.csr_list[id]->v_weight[v + 1]; index++) {
							//! Get weight of (v,n)
							int64_t n = info.context.csr_list[id]->e[index];
							CheckUpdateDistance<T>(v, n, info.context.csr_list[id]->w_double[index], modified, dists,
							                       changed);
						}
					} else {
						for (auto index = (int64_t)info.context.csr_list[id]->v_weight[v];
							 index < (int64_t)info.context.csr_list[id]->v_weight[v + 1]; index++) {
							//! Get weight of (v,n)
							int64_t n = info.context.csr_list[id]->e[index];
							CheckUpdateDistance<T>(v, n, info.context.csr_list[id]->w[index], modified, dists, changed);
						}
					}
				}
			}
		}
		for (idx_t i = 0; i < args.size(); i++) {
			auto target_index = vdata_target.sel->get_index(i);

			if (!vdata_target.validity.RowIsValid(target_index)) {
				result_validity.SetInvalid(i);
			}
			const auto &target_entry = target_data[target_index];
			auto resulting_distance = dists[target_entry][i];
			if (is_double) {
				if (resulting_distance == DBL_MAX) {
					result_validity.SetInvalid(i);
				} else {
					result_data[i] = resulting_distance;
				}
			} else {
				if (resulting_distance == INT64_MAX) {
					result_validity.SetInvalid(i);
				} else {
					result_data[i] = resulting_distance;
				}
			}

		}
		result_size += curr_batch_size;
	}
}


static void CheapestPathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CheapestPathBindData &)*func_expr.bind_info;

	int32_t id = args.data[0].GetValue(0).GetValue<int32_t>();
	int64_t input_size = args.data[1].GetValue(0).GetValue<int64_t>();

	auto &src = args.data[2];

	VectorData vdata_src, vdata_target;
	src.Orrify(args.size(), vdata_src);

	auto src_data = (int64_t *)vdata_src.data;

	auto &target = args.data[3];
	target.Orrify(args.size(), vdata_target);
	auto target_data = (int64_t *)vdata_target.data;
//	std::cout << "LANES: " << info.context.lane_limit << std::endl;

//	auto start = high_resolution_clock::now();
	if (info.context.csr_list[id]->w.empty()) {
		TemplatedBellmanFord<double_t>(info, args, input_size, result, vdata_src, src_data, vdata_target, target_data,
		                               id, true);
	} else {
		TemplatedBellmanFord<int64_t>(info, args, input_size, result, vdata_src, src_data, vdata_target, target_data,
		                              id, false);
	}
//	auto stop = high_resolution_clock::now();

//	auto duration = duration_cast<microseconds>(stop - start);

// 	std::cout << "Total time: " <<  duration.count() << std::endl;


}

static unique_ptr<FunctionData> CheapestPathBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {

	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}

	int32_t id = ExpressionExecutor::EvaluateScalar(*arguments[0]).GetValue<int32_t>();
	if ((uint64_t)id + 1 > context.csr_list.size()) {
		throw ConstraintException("Invalid ID");
	}
	auto csr_entry = context.csr_list.find((uint64_t)id);
	if (csr_entry == context.csr_list.end()) {
		throw ConstraintException("Need to initialize CSR before doing cheapest path");
	}

	if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e && csr_entry->second->initialized_w)) {
		throw ConstraintException("Need to initialize CSR before doing cheapest path");
	}
	string file_name;

	if (context.csr_list[id]->w.empty()) {
		bound_function.return_type = LogicalType::DOUBLE;
	} else {
		bound_function.return_type = LogicalType::BIGINT;
	}

	return make_unique<CheapestPathBindData>(context, file_name);
}

CreateScalarFunctionInfo SQLPGQFunctions::GetCheapestPathFunction() {
	ScalarFunctionSet set("cheapest_path");


	set.AddFunction(ScalarFunction(
	    {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::ANY, CheapestPathFunction, false, CheapestPathBind));

	return CreateScalarFunctionInfo(set);
}
} // namespace duckdb