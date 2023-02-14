//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckdb/parser/expression/property_graph_table_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Represents a reference to a graph table from the CREATE PROPERTY GRAPH
class PropertyGraphTable {
public:
	//! Specify both the column and table name
	PropertyGraphTable(string table_name, vector<string> column_name, vector<string> label);

	string table_name;

	//! The stack of names in order of which they appear (column_names[0].column_names[1].column_names[2]....)
	vector<string> column_names;

	vector<string> labels;

	bool is_vertex_table;

	string discriminator;
};
} // namespace duckdb
