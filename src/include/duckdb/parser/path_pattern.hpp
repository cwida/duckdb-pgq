
#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class PathPattern {
public: 
	vector<unique_ptr<PathElement>> path_elements;
public:
	PathPattern();

	unique_ptr<ParsedExpression> where_clause;

	bool Equals(const PathPattern *other_p) const;

	void Serialize(Serializer &serializer) const;

	static unique_ptr<PathPattern> Deserialize(Deserializer &deserializer);

};


}