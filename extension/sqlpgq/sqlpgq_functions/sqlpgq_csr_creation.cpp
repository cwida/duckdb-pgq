//
// Created by daniel on 19-4-22.
//

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_functions.hpp"
#include "sqlpgq_common.hpp"

#include <chrono>
#include <mutex>
#include <thread>

namespace duckdb {



static void CsrInitializeVertex(ClientContext &context, int32_t id, int64_t v_size) {
	lock_guard<mutex> csr_init_lock(context.csr_lock);

	auto csr_entry = context.csr_list.find(id);
	if (csr_entry != context.csr_list.end()) {
		if (csr_entry->second->initialized_v) {
			return;
		}
	}
	try {
		auto csr = make_unique<Csr>();
		// extra 2 spaces required for CSR padding
		// data contains a vector of elements so will need an anonymous function to apply the
		// the first element id is repeated across, can I access the value directly?
		csr->v = new std::atomic<int64_t>[v_size + 2];

		for (idx_t i = 0; i < (idx_t)v_size + 1; i++) {
			csr->v[i] = 0;
		}
		csr->initialized_v = true;

		if (csr_entry != context.csr_list.end()) {
			context.csr_list[id] = move(csr);
		} else {
			context.csr_list.insert({id, move(csr)});
		}

	} catch (std::bad_alloc const &) {
		throw Exception("Unable to initialise vector of size for csr vertex table representation");
	};

	return;
}

static void CsrInitializeEdge(ClientContext &context, int32_t id, int64_t v_size, int64_t e_size) {
	const lock_guard<mutex> csr_init_lock(context.csr_lock);

	auto csr_entry = context.csr_list.find(id);
	if (csr_entry->second->initialized_e) {
		return;
	}
	try {
		csr_entry->second->e.resize(e_size, 0);
	} catch (std::bad_alloc const &) {
		throw Exception("Unable to initialise vector of size for csr edge table representation");
	}
	for (auto i = 1; i < v_size + 2; i++) {
		csr_entry->second->v[i] += csr_entry->second->v[i - 1];
	}
	csr_entry->second->initialized_e = true;
	return;
}

static void CsrInitializeWeight(ClientContext &context, int32_t id, int64_t v_size, int64_t e_size,
                                PhysicalType weight_type) {
	const lock_guard<mutex> csr_init_lock(context.csr_lock);
	auto csr_entry = context.csr_list.find(id);

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
	auto csr_entry = info.context.csr_list.find(info.id);

	if (csr_entry == info.context.csr_list.end()) {
		CsrInitializeVertex(info.context, info.id, input_size);
		csr_entry = info.context.csr_list.find(info.id);
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

	auto csr_entry = info.context.csr_list.find(info.id);
	if (!csr_entry->second->initialized_e) {
		CsrInitializeEdge(info.context, info.id, vertex_size, edge_size);
	}
	if (info.weight_type == LogicalType::SQLNULL) {
		BinaryExecutor::Execute<int64_t, int64_t, int32_t>(args.data[3], args.data[4], result, args.size(),
		                                                   [&](int64_t src, int64_t dst) {
			                                                   auto pos = ++csr_entry->second->v[src + 1];
			                                                   csr_entry->second->e[(int64_t)pos - 1] = dst;
			                                                   return 1;
		                                                   });
	} else {
		auto weight_type = args.data[5].GetType().InternalType();
		if (!csr_entry->second->initialized_w) {
			CsrInitializeWeight(info.context, info.id, vertex_size, edge_size, args.data[5].GetType().InternalType());
		}
		if (weight_type == PhysicalType::INT64) {
			TernaryExecutor::Execute<int64_t, int64_t, int64_t, int32_t>(
			    args.data[3], args.data[4], args.data[5], result, args.size(),
			    [&](int64_t src, int64_t dst, int64_t weight) {
				    auto pos = ++csr_entry->second->v[src + 1];
				    csr_entry->second->e[(int64_t)pos - 1] = dst;
				    csr_entry->second->w[(int64_t)pos - 1] = weight;
				    return weight;
			    });
		} else if (weight_type == PhysicalType::DOUBLE) {
			TernaryExecutor::Execute<int64_t, int64_t, double_t, int32_t>(
			    args.data[3], args.data[4], args.data[5], result, args.size(),
			    [&](int64_t src, int64_t dst, double_t weight) {
				    auto pos = ++csr_entry->second->v[src + 1];
				    csr_entry->second->e[(int64_t)pos - 1] = dst;
				    csr_entry->second->w_double[(int64_t)pos - 1] = weight;
				    return weight;
			    });
		}
	}

	return;
}

//static unique_ptr<FunctionData> CreateCsrEdgeBind(ClientContext &context, ScalarFunction &bound_function,
//                                                  vector<unique_ptr<Expression>> &arguments) {
//	if (!arguments[0]->IsFoldable()) {
//		throw InvalidInputException("Id must be constant.");
//	}
//	Value id = ExpressionExecutor::EvaluateScalar(*arguments[0]);
//	if (arguments.size() == 6) {
//		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[5]->return_type);
//	} else {
//		auto logical_type = LogicalType::SQLNULL;
//		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
//	}
//}

// static unique_ptr<FunctionData> CreateCsrWeightBind(ClientContext &context, ScalarFunction &bound_function,
//                                                     vector<unique_ptr<Expression>> &arguments) {
//	if (!arguments[0]->IsFoldable()) {
//		throw InvalidInputException("Id must be constant.");
//	}
//	Value id = ExpressionExecutor::EvaluateScalar(*arguments[0]);
//	return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), true);
// }

//static unique_ptr<FunctionData> CreateCsrVertexBind(ClientContext &context, ScalarFunction &bound_function,
//                                                    vector<unique_ptr<Expression>> &arguments) {
//	if (!arguments[0]->IsFoldable()) {
//		throw InvalidInputException("Id must be constant.");
//	}
//
//	Value id = ExpressionExecutor::EvaluateScalar(*arguments[0]);
//	if (arguments.size() == 4) {
//		auto logical_type = LogicalType::SQLNULL;
//		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
//	} else {
//		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[3]->return_type);
//	}
//}

CreateScalarFunctionInfo SQLPGQFunctions::GetCsrVertexFunction() {
	ScalarFunctionSet set("create_csr_vertex");

	set.AddFunction(ScalarFunction(
	    "create_csr_vertex", {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::BIGINT, CreateCsrVertexFunction, false, CreateCsrVertexBind));

	return CreateScalarFunctionInfo(set);
}

CreateScalarFunctionInfo SQLPGQFunctions::GetCsrEdgeFunction() {
	ScalarFunctionSet set("create_csr_edge");
	set.AddFunction(ScalarFunction(
	    {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::INTEGER, CreateCsrEdgeFunction, false, CreateCsrEdgeBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::BIGINT},
	                               LogicalType::INTEGER, CreateCsrEdgeFunction, false, CreateCsrEdgeBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::DOUBLE},
	                               LogicalType::INTEGER, CreateCsrEdgeFunction, false, CreateCsrEdgeBind));

	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
