#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_common.hpp"
#include "sqlpgq_functions.hpp"
#include "duckdb/common/compressed_sparse_row.hpp"
#include <chrono>
#include <math.h>
#include <mutex>

namespace duckdb {

enum class CSRWType : int32_t {
	// possible weight types of a csr
	UNWEIGHTED,   //! unweighted
	INTWEIGHT,    //! integer
	DOUBLEWEIGHT, //! double
};

static void GetCsrWTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CSRFunctionData &)*func_expr.bind_info;

	auto sqlpgq_state_entry = info.context.registered_state.find("sqlpgq");
	if (sqlpgq_state_entry == info.context.registered_state.end()) {
		//! Wondering how you can get here if the extension wasn't loaded, but leaving this check in anyways
		throw MissingExtensionException("The SQL/PGQ extension has not been loaded");
	}
	auto sqlpgq_state = reinterpret_cast<SQLPGQContext *>(sqlpgq_state_entry->second.get());
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<int32_t>(result);
	auto csr = sqlpgq_state->GetCSR(info.id);
	int32_t flag;
	if (!csr->initialized_w) {
		flag = (int32_t)CSRWType::UNWEIGHTED;
	} else if (csr->w.size()) {
		flag = (int32_t)CSRWType::INTWEIGHT;
	} else if (csr->w_double.size()) {
		flag = (int32_t)CSRWType::DOUBLEWEIGHT;
	}
	result_data[0] = flag;
}

CreateScalarFunctionInfo SQLPGQFunctions::GetGetCsrWTypeFunction() {
	ScalarFunctionSet set("sqlpgq_csr_get_w_type");

	set.AddFunction(ScalarFunction("sqlpgq_csr_get_w_type", {LogicalType::INTEGER}, LogicalType::INTEGER,
	                               GetCsrWTypeFunction, CSRFunctionData::CSRBind));

	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
