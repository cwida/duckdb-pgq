//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlpgq_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

class SQLPGQContext : public ClientContextState {
public:
    explicit SQLPGQContext() {

    }

    void InsertNewPropertyGraph() {

    }

    void InsertNewCSR(int32_t id, unique_ptr<CSR> csr) {
        csr_list[id] = std::move(csr);
    }
    void EraseCSR(int32_t id) {
        csr_list.erase(id);
    }

    void QueryEnd() override {
        // Check if it contains a path query


    }

protected:
    //! Property graphs that are registered
    std::unordered_map<string, unique_ptr<CreateInfo>> registered_property_graphs;

    //! Used to build the CSR data structures required for path-finding queries
    std::unordered_map<int32_t, unique_ptr<CSR>> csr_list;
    std::mutex csr_lock;
};

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

struct IterativeLengthFunctionData : public FunctionData {
public:
	ClientContext &context;
	int32_t csr_id;

	IterativeLengthFunctionData(ClientContext &context, int32_t csr_id) : context(context), csr_id(csr_id) {
	}
	static unique_ptr<FunctionData> IterativeLengthBind(ClientContext &context, ScalarFunction &bound_function,
	                                                    vector<unique_ptr<Expression>> &arguments);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

struct CheapestPathLengthFunctionData : public FunctionData {
	ClientContext &context;
	int32_t csr_id;

	CheapestPathLengthFunctionData(ClientContext &context, int32_t csr_id) : context(context), csr_id(csr_id) {
	}
	static unique_ptr<FunctionData> CheapestPathLengthBind(ClientContext &context, ScalarFunction &bound_function,
	                                                       vector<unique_ptr<Expression>> &arguments);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};




} // namespace duckdb
