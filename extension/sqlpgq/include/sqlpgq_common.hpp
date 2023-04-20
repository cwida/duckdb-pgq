//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlpgq_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct CSRFunctionData : public FunctionData {
public:
	CSRFunctionData(ClientContext &context, int32_t id, LogicalType weight_type);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
	static unique_ptr<FunctionData> CSRVertexBind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);
	static unique_ptr<FunctionData> CSREdgeBind(ClientContext &context, ScalarFunction &bound_function,
	                                                  vector<unique_ptr<Expression>> &arguments);
	public:
	ClientContext &context;
	const int32_t id;
	const LogicalType weight_type; // TODO Make sure type is LogicalType::SQLNULL when no type is provided

};
} // namespace duckdb

