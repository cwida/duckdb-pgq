#include "sqlpgq_common.hpp"

#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <utility>

namespace duckdb {

CSRFunctionData::CSRFunctionData(ClientContext &context, int32_t id, LogicalType weight_type)
    : context(context), id(id), weight_type(std::move(weight_type)) {
}

unique_ptr<FunctionData> CSRFunctionData::Copy() const {
	return make_unique<CSRFunctionData>(context, id, weight_type);
}

bool CSRFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = (const CSRFunctionData &)other_p;
	return id == other.id && weight_type == other.weight_type;
}

unique_ptr<FunctionData> CSRFunctionData::CSRVertexBind(ClientContext &context, ScalarFunction &bound_function,
                                                        vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}

	Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (arguments.size() == 4) {
		auto logical_type = LogicalType::SQLNULL;
		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
	} else {
		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[3]->return_type);
	}
}

unique_ptr<FunctionData> CSRFunctionData::CSREdgeBind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}
	Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (arguments.size() == 6) {
		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[5]->return_type);
	} else {
		auto logical_type = LogicalType::SQLNULL;
		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
	}
}

unique_ptr<FunctionData> IterativeLengthFunctionData::Copy() const {
	return make_unique<IterativeLengthFunctionData>(context);
}

bool IterativeLengthFunctionData::Equals(const FunctionData &other_p) const {
	// TODO: Change this to check if both are on same CSR
	return true;
}

unique_ptr<FunctionData> IterativeLengthFunctionData::IterativeLengthBind(ClientContext &context,
                                                                          ScalarFunction &bound_function,
                                                                          vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}

	int32_t id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]).GetValue<int32_t>();
	if ((uint64_t)id + 1 > context.client_data->csr_list.size()) {
		throw ConstraintException("Invalid ID");
	}
	auto csr_entry = context.client_data->csr_list.find((uint64_t)id);
	if (csr_entry == context.client_data->csr_list.end()) {
		throw ConstraintException("Need to initialize CSR before doing shortest path");
	}

	if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
		throw ConstraintException("Need to initialize CSR before doing shortest path");
	}


	return make_unique<IterativeLengthFunctionData>(context);
}

unique_ptr<FunctionData>
CheapestPathLengthFunctionData::CheapestPathLengthBind(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments) {

	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}

	int32_t id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]).GetValue<int32_t>();
	if ((uint64_t)id + 1 > context.client_data->csr_list.size()) {
		throw ConstraintException("Invalid ID");
	}
	auto csr_entry = context.client_data->csr_list.find((uint64_t)id);
	if (csr_entry == context.client_data->csr_list.end()) {
		throw ConstraintException("Need to initialize CSR before doing cheapest path");
	}

	if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e && csr_entry->second->initialized_w)) {
		throw ConstraintException("Need to initialize CSR before doing cheapest path");
	}

	if (context.client_data->csr_list[id]->w.empty()) {
		bound_function.return_type = LogicalType::DOUBLE;
	} else {
		bound_function.return_type = LogicalType::BIGINT;
	}

	return make_unique<CheapestPathLengthFunctionData>(context);
}

unique_ptr<FunctionData> CheapestPathLengthFunctionData::Copy() const {
	return make_unique<CheapestPathLengthFunctionData>(context);
}

bool CheapestPathLengthFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = (const CheapestPathLengthFunctionData &)other_p;
	return false;
}

} // namespace duckdb
