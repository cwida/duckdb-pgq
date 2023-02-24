
#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/graph_element_pattern.hpp"
#include "duckdb/common/vector.hpp"


namespace duckdb {

class MatchRef : public TableRef {
public:
	MatchRef() : TableRef(TableReferenceType::MATCH) {

	}

	string pg_name;
	vector<unique_ptr<GraphElementPattern>> path_list;

	vector<unique_ptr<ParsedExpression>> column_list;

	unique_ptr<ParsedExpression> where_clause;

public:
	string ToString() const override;
	bool Equals(const TableRef *other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a DummyTableRef
	void Serialize(FieldWriter &serializer) const override;
	//! Deserializes a blob back into a DummyTableRef
	static unique_ptr<TableRef> Deserialize(FieldReader &source);

};


}