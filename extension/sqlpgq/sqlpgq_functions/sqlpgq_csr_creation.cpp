#include "duckdb/common/vector_operations/quaternary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_common.hpp"
#include "sqlpgq_functions.hpp"

#include <chrono>
#include <math.h>
#include <mutex>

namespace duckdb {

static void CsrInitializeVertex(ClientContext &context, int32_t id, int64_t v_size) {
	lock_guard<mutex> csr_init_lock(context.client_data->csr_lock);

	auto csr_entry = context.client_data->csr_list.find(id);
	if (csr_entry != context.client_data->csr_list.end()) {
		if (csr_entry->second->initialized_v) {
			return;
		}
	}
	try {
		auto csr = make_unique<CSR>();
		// extra 2 spaces required for CSR padding
		// data contains a vector of elements so will need an anonymous function to apply the
		// first element id is repeated across, can I access the value directly?
		csr->v = new std::atomic<int64_t>[v_size + 2];

		for (idx_t i = 0; i < (idx_t)v_size + 1; i++) {
			csr->v[i] = 0;
		}
		csr->initialized_v = true;

		if (csr_entry != context.client_data->csr_list.end()) {
			context.client_data->csr_list[id] = std::move(csr);
		} else {
			context.client_data->csr_list.insert({id, std::move(csr)});
		}

	} catch (std::bad_alloc const &) {
		throw Exception("Unable to initialise vector of size for csr vertex table representation");
	}

	return;
}

static void CsrInitializeEdge(ClientContext &context, int32_t id, int64_t v_size, int64_t e_size) {
	const lock_guard<mutex> csr_init_lock(context.client_data->csr_lock);

	auto csr_entry = context.client_data->csr_list.find(id);
	if (csr_entry->second->initialized_e) {
		return;
	}
	try {
		csr_entry->second->e.resize(e_size, 0);
		csr_entry->second->edge_ids.resize(e_size, 0);
	} catch (std::bad_alloc const &) {
		throw Exception("Unable to initialise vector of size for csr edge table representation");
	}
	for (auto i = 1; i < v_size + 2; i++) {
		csr_entry->second->v[i] += csr_entry->second->v[i - 1];
	}
	csr_entry->second->initialized_e = true;
	return;
}

static void CsrInitializeWeight(ClientContext &context, int32_t id, int64_t e_size, PhysicalType weight_type) {
	const lock_guard<mutex> csr_init_lock(context.client_data->csr_lock);
	auto csr_entry = context.client_data->csr_list.find(id);

	if (csr_entry->second->initialized_w) {
		return;
	}
	try {
		if (weight_type == PhysicalType::INT64) {
			csr_entry->second->w.resize(e_size, 0);
		} else if (weight_type == PhysicalType::DOUBLE) {
			csr_entry->second->w_double.resize(e_size, 0);
		} else {
			throw NotImplementedException("Unrecognized weight type detected.");
		}
	} catch (std::bad_alloc const &) {
		throw Exception("Unable to initialise vector of size for csr weight table representation");
	}

	csr_entry->second->initialized_w = true;
	return;
}

static void CreateCsrVertexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CSRFunctionData &)*func_expr.bind_info;

	int64_t input_size = args.data[1].GetValue(0).GetValue<int64_t>();
	auto csr_entry = info.context.client_data->csr_list.find(info.id);

	if (csr_entry == info.context.client_data->csr_list.end()) {
		CsrInitializeVertex(info.context, info.id, input_size);
		csr_entry = info.context.client_data->csr_list.find(info.id);
	} else {
		if (!csr_entry->second->initialized_v) {
			CsrInitializeVertex(info.context, info.id, input_size);
		}
	}

	BinaryExecutor::Execute<int64_t, int64_t, int64_t>(args.data[2], args.data[3], result, args.size(),
	                                                   [&](int64_t src, int64_t cnt) {
		                                                   int64_t edge_count = 0;
		                                                   csr_entry->second->v[src + 2] = cnt;
		                                                   edge_count = edge_count + cnt;
		                                                   return edge_count;
	                                                   });

	return;
}

static void CreateCsrEdgeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CSRFunctionData &)*func_expr.bind_info;

	int64_t vertex_size = args.data[1].GetValue(0).GetValue<int64_t>();
	int64_t edge_size = args.data[2].GetValue(0).GetValue<int64_t>();

	auto csr_entry = info.context.client_data->csr_list.find(info.id);
	if (!csr_entry->second->initialized_e) {
		CsrInitializeEdge(info.context, info.id, vertex_size, edge_size);
	}
	if (info.weight_type == LogicalType::SQLNULL) {
		TernaryExecutor::Execute<int64_t, int64_t, int64_t, int32_t>(
		    args.data[3], args.data[4], args.data[5], result, args.size(),
		    [&](int64_t src, int64_t dst, int64_t edge_id) {
			    auto pos = ++csr_entry->second->v[src + 1];
			    csr_entry->second->e[(int64_t)pos - 1] = dst;
			    csr_entry->second->edge_ids[(int64_t)pos - 1] = edge_id;
			    return 1;
		    });
		return;
	}
	auto weight_type = args.data[5].GetType().InternalType();
	if (!csr_entry->second->initialized_w) {
		CsrInitializeWeight(info.context, info.id, edge_size, args.data[5].GetType().InternalType());
	}
	if (weight_type == PhysicalType::INT64) {
		QuaternaryExecutor::Execute<int64_t, int64_t, int64_t, int64_t, int32_t>(
		    args.data[3], args.data[4], args.data[5], args.data[6], result, args.size(),
		    [&](int64_t src, int64_t dst, int64_t edge_id, int64_t weight) {
			    auto pos = ++csr_entry->second->v[src + 1];
			    csr_entry->second->e[(int64_t)pos - 1] = dst;
			    csr_entry->second->edge_ids[(int64_t)pos - 1] = edge_id;
			    csr_entry->second->w[(int64_t)pos - 1] = weight;
			    return weight;
		    });
		return;
	}

	QuaternaryExecutor::Execute<int64_t, int64_t, int64_t, double_t, int32_t>(
	    args.data[3], args.data[4], args.data[5], args.data[6], result, args.size(),
	    [&](int64_t src, int64_t dst, int64_t edge_id, double_t weight) {
		    auto pos = ++csr_entry->second->v[src + 1];
		    csr_entry->second->e[(int64_t)pos - 1] = dst;
		    csr_entry->second->edge_ids[(int64_t)pos - 1] = edge_id;
		    csr_entry->second->w_double[(int64_t)pos - 1] = weight;
		    return weight;
	    });

	return;
}

CreateScalarFunctionInfo SQLPGQFunctions::GetCsrVertexFunction() {
	ScalarFunctionSet set("create_csr_vertex");

	set.AddFunction(ScalarFunction(
	    "create_csr_vertex", {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::BIGINT, CreateCsrVertexFunction, CSRFunctionData::CSRVertexBind));

	return CreateScalarFunctionInfo(set);
}

CreateScalarFunctionInfo SQLPGQFunctions::GetCsrEdgeFunction() {
	ScalarFunctionSet set("create_csr_edge");
	//! No edge weight
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::BIGINT},
	                               LogicalType::INTEGER, CreateCsrEdgeFunction, CSRFunctionData::CSREdgeBind));

	//! Integer for edge weight
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                               LogicalType::INTEGER, CreateCsrEdgeFunction, CSRFunctionData::CSREdgeBind));

	//! Double for edge weight
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE},
	                               LogicalType::INTEGER, CreateCsrEdgeFunction, CSRFunctionData::CSREdgeBind));

	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb