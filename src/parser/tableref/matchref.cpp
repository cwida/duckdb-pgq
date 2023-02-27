
#include "duckdb/parser/tableref/matchref.hpp"

namespace duckdb {

string MatchRef::ToString() const {
    // TODO Implement

	return "";
};
bool MatchRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}

	auto other = (MatchRef *)other_p;
	if (pg_name != other->pg_name) {
		return false;
	}

	// path_list
	for (idx_t i = 0; i < path_list.size(); i++) {
		if (!path_list[i]->Equals(other->path_list[i].get())) {
			return false;
		}
	}

	// columns
	if (column_list != other->column_list) {
		return false;
	}

	// where clause
	if (!where_clause->Equals(other->where_clause.get())) {
		return false;
	}

	return true;
};

unique_ptr<TableRef> MatchRef::Copy() {
	auto copy = make_unique<MatchRef>();
	copy->pg_name = pg_name;

	for (auto &path : path_list) {
		copy->path_list.push_back(path->Copy());
	}

	for (auto &column : column_list) {
		copy->column_list.push_back(column->Copy());
	}

	copy->where_clause = std::move(where_clause);

	return std::move(copy);
}

void MatchRef::Serialize(FieldWriter &writer) const {
	writer.WriteString(pg_name);
	writer.WriteSerializableList<PathPattern>(path_list);
	writer.WriteSerializableList<ParsedExpression>(column_list);
	writer.WriteOptional(where_clause);
}

unique_ptr<TableRef> MatchRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<MatchRef>();

	result->pg_name = reader.ReadRequired<string>();
	result->path_list = reader.ReadRequiredSerializableList<PathPattern>();
	result->column_list = reader.ReadRequiredSerializableList<ParsedExpression>();
	result->where_clause = reader.ReadOptional<ParsedExpression>(nullptr);
	return result;
}

}