
#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class GraphElementPattern {
public:
	GraphElementPattern();


	unique_ptr<ParsedExpression> where_clause;


	void Serialize(Serializer &serializer) const;

	static unique_ptr<GraphElementPattern> Deserialize(Deserializer &deserializer);

};


}