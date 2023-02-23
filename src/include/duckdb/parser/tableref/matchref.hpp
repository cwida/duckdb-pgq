
#pragma once

#include "duckdb/parser/graph_element_pattern.hpp"

namespace duckdb {

class MatchRef : public TableRef {
public:
	MatchRef() : TableRef(TableReferenceType::MATCH) {

	}

	string pg_name;
	vector<unique_ptr<GraphElementPattern>> path_list;

	vector<unique_ptr<ParsedExpression>> column;

	unique_ptr<ParsedExpression> where_clause;


};


}