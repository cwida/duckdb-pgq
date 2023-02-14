//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckdb/parser/expression/property_graph_table_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Represents a reference to a graph table from the CREATE PROPERTY GRAPH
class PropertyGraphTable {
public:
	//! Specify both the column and table name
	PropertyGraphTable(string column_name, string label, string table_name);
	//! Only specify the column name, the table name will be derived later
	explicit PropertyGraphTable(string column_name, string label);
	//! Specify a set of names
	explicit PropertyGraphTable(vector<string> column_names, vector<string> labels);

	//! The stack of names in order of which they appear (column_names[0].column_names[1].column_names[2]....)
	vector<string> column_names;

	vector<string> labels;

	bool is_vertex_table;

	string discriminator;
};
} // namespace duckdb
