#include "sqlpgq_common.hpp"

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

	Value id = ExpressionExecutor::EvaluateScalar(*arguments[0]);
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
	Value id = ExpressionExecutor::EvaluateScalar(*arguments[0]);
	if (arguments.size() == 6) {
		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[5]->return_type);
	} else {
		auto logical_type = LogicalType::SQLNULL;
		return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
	}
}


unique_ptr<FunctionData> IterativeLengthFunctionData::Copy() const {
	return make_unique<IterativeLengthFunctionData>(context, file_name);
}

bool IterativeLengthFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = (const IterativeLengthFunctionData &)other_p;
	return file_name == other.file_name;
}

static unique_ptr<FunctionData> IterativeLengthFunctionData::IterativeLengthBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	string file_name;
	if (arguments.size() == 5) {
		file_name = ExpressionExecutor::EvaluateScalar(*arguments[4]).GetValue<string>();
	} else {
		file_name = "timings-test.txt";
	}
	return make_unique<IterativeLengthFunctionData>(context, file_name);
}

} // namespace duckdb