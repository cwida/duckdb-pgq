
#include "duckdb/parser/tableref/matchref.hpp"

namespace duckdb {

unique_ptr<TableRef> MatchRef::Copy() {
	auto copy = make_unique<MatchRef>();
	copy->pg_name = pg_name;

	for (auto &path : path_list) {
		copy->path_list.push_back(std::move(path));
	}

	for (auto &column : column_list) {
		copy->column_list.push_back(std::move(column));
	}

	copy->where_clause = std::move(where_clause);

	return std::move(copy);
}

void MatchRef::Serialize(FieldWriter &writer) const {
	writer.WriteString(pg_name);
	writer.WriteSerializableList<GraphElementPattern>(path_list);
	writer.WriteSerializableList<ParsedExpression>(column_list);
	writer.WriteSerializable<ParsedExpression>(*where_clause);
}

unique_ptr<TableRef> MatchRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<MatchRef>();

	result->pg_name = reader.ReadRequired<string>();
	result->path_list = reader.ReadRequiredSerializableList<GraphElementPattern>();
	result->column_list = reader.ReadRequiredSerializableList<ParsedExpression>();
	result->where_clause = reader.ReadRequiredSerializable<ParsedExpression>();
}

}