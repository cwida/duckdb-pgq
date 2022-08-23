#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_functions.hpp"
#include <algorithm>
#include <chrono>

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
		dists[i] = std::vector<T>(args.size(), std::numeric_limits<T>::max()/2);
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
int64_t UpdateOneLane(T &n_dist, T v_dist, T weight) {
	T new_dist = v_dist + weight;
	bool better = new_dist < n_dist;
	T min = better ? new_dist : n_dist;
	T diff = n_dist ^ min;
	n_dist = min;
	return diff;
}

template <typename T>
bool UpdateLanes(std::unordered_map<int64_t, vector<T>> &dists, size_t v, T n, T weight) {
	std::vector<T> &v_dists = dists[v];
	std::vector<T> &n_dists = dists[n];
	size_t num_lanes = dists[v].size();
	size_t lane_idx = 0;
	int64_t xor_diff = 0;
	while (lane_idx < num_lanes) {
		xor_diff |= UpdateOneLane<T>(n_dists[lane_idx], v_dists[lane_idx], weight);
		++lane_idx;
	}
	return xor_diff != 0;
}


template <typename T>
void TemplatedBellmanFord(CheapestPathBindData &info, DataChunk &args, int64_t input_size, Vector &result,
                          VectorData vdata_src, int64_t *src_data, const VectorData &vdata_target, int64_t *target_data,
                          int32_t id, std::vector<T> weight_array) {
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
				for (auto index = (int64_t)info.context.csr_list[id]->v_weight[v]; index < (int64_t)info.context.csr_list[id]->v_weight[v + 1]; index++) {
				changed = UpdateLanes<T>(dists, v, info.context.csr_list[id]->e[index], weight_array[index]) | changed;
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

			if (resulting_distance == std::numeric_limits<T>::max()/2) {
				result_validity.SetInvalid(i);
			} else {
				result_data[i] = resulting_distance;
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

	if (!info.context.csr_list[id]->w_bigint.empty()) {
		TemplatedBellmanFord<int64_t>(info, args, input_size, result, vdata_src, src_data, vdata_target, target_data,
		                              id, info.context.csr_list[id]->w_bigint);
	} else if (!info.context.csr_list[id]->w_double.empty()) {
		throw NotImplementedException("Cheapest path using doubles has not been implemented yet.");
//		TemplatedBellmanFord<double_t>(info, args, input_size, result, vdata_src, src_data, vdata_target, target_data,
//		                              id, info.context.csr_list[id]->w_double);
	}
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

	if (!context.csr_list[id]->w_bigint.empty()) {
		bound_function.return_type = LogicalType::BIGINT;
	} else if (!context.csr_list[id]->w_double.empty()) {
		bound_function.return_type = LogicalType::DOUBLE;
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