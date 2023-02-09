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

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class PropertyGraphTableExpression : public ParsedExpression {
public:
	//! Specify both the column and table name
	PropertyGraphTableExpression(string column_name, string table_name);
	//! Only specify the column name, the table name will be derived later
	explicit PropertyGraphTableExpression(string column_name);
	//! Specify a set of names
	explicit PropertyGraphTableExpression(vector<string> column_names);

	//! The stack of names in order of which they appear (column_names[0].column_names[1].column_names[2]....)
	vector<string> column_names;

	vector<string> labels;

public:
	bool IsQualified() const;
	const string &GetColumnName() const;
	const string &GetTableName() const;
	bool IsScalar() const override {
		return false;
	}

	string GetName() const override;
	string ToString() const override;

	static bool Equal(const PropertyGraphTableExpression *a, const PropertyGraphTableExpression *b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};
} // namespace duckdb
