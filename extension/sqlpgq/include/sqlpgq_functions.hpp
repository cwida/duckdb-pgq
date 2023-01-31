//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlpgq_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

#define LANE_LIMIT         512
#define VISIT_SIZE_DIVISOR 2

class SQLPGQFunctions {
public:
	static vector<CreateScalarFunctionInfo> GetFunctions() {
		vector<CreateScalarFunctionInfo> functions;

		// Create functions
		functions.push_back(GetCsrVertexFunction());
		functions.push_back(GetCsrEdgeFunction());
		functions.push_back(GetCheapestPathLengthFunction());
		functions.push_back(GetShortestPathFunction());
		functions.push_back(GetShortestPath2Function());
		functions.push_back(GetReachabilityFunction());
		functions.push_back(GetIterativeLengthFunction());
		functions.push_back(GetIterativeLengthBidirectionalFunction());
		functions.push_back(GetIterativeLength2Function());
		
		return functions;
	}

private:
	static CreateScalarFunctionInfo GetCsrVertexFunction();
	static CreateScalarFunctionInfo GetCsrEdgeFunction();
	static CreateScalarFunctionInfo GetCheapestPathLengthFunction();
	static CreateScalarFunctionInfo GetShortestPathFunction();
	static CreateScalarFunctionInfo GetShortestPath2Function();
	static CreateScalarFunctionInfo GetReachabilityFunction();
	static CreateScalarFunctionInfo GetIterativeLengthFunction();

	static CreateScalarFunctionInfo GetIterativeLengthBidirectionalFunction();
	static CreateScalarFunctionInfo GetIterativeLength2Function();


	static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
	                       vector<CreateScalarFunctionInfo> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}
};

} // namespace duckdb
