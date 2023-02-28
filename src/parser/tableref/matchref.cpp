
#include "duckdb/parser/tableref/matchref.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

string MatchRef::ToString() const {
    string result = "GRAPH_TABLE (";
	result += pg_name + ", MATCH";

	for (idx_t i = 0; i < path_list.size(); i++) {
		if (i > 0) {
			result += ",";
		}
		for (idx_t j = 0; j < path_list[i]->path_elements.size(); j++) {
			auto &path_element = path_list[i]->path_elements[j];
			if (path_element->match_type == PGQMatchType::MATCH_VERTEX) {
				result += "(" + path_element->variable_binding + ":" + path_element->label + ")";
			} else {
				// TODO Implement for the edges
			}
		}
	}
	if (where_clause) {
		result += "WHERE " + where_clause->ToString();
	}

	result += "\nCOLUMNS (";
	for (idx_t i = 0; i < column_list.size(); i++) {
		auto &column = (ColumnRefExpression &)*column_list[i];
		result += i > 0 ? ", " + column.column_names[0] + "." + column.column_names[1]
		     : column.column_names[0] + "." + column.column_names[1];
	}
	result += ")\n";
	result += ")" + alias;

	return result;
};
bool MatchRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}

	auto other = (MatchRef *)other_p;
	if (pg_name != other->pg_name) {
		return false;
	}

	if (alias != other->alias) {
		return false;
	}

	if (path_list.size() != other->path_list.size()) {
		return false;
	}

	// path_list
	for (idx_t i = 0; i < path_list.size(); i++) {
		if (!path_list[i]->Equals(other->path_list[i].get())) {
			return false;
		}
	}

	if (column_list.size() != column_list.size()) {
		return false;
	}

	// columns
	for (idx_t i = 0; i < column_list.size(); i++) {
		if (!column_list[i]->Equals(other->column_list[i].get())) {
			return false;
		}
	}

	// where clause
	if (where_clause && other->where_clause.get()) {
		if (!where_clause->Equals(other->where_clause.get())) {
			return false;
		}
	}
	if ((where_clause && !other->where_clause.get())
	    || (!where_clause && other->where_clause.get())) {
		return false;
	}

	return true;
};

unique_ptr<TableRef> MatchRef::Copy() {
	auto copy = make_unique<MatchRef>();
	copy->pg_name = pg_name;
	copy->alias = alias;

	for (auto &path : path_list) {
		copy->path_list.push_back(path->Copy());
	}

	for (auto &column : column_list) {
		copy->column_list.push_back(column->Copy());
	}
	if (where_clause) {
		copy->where_clause = where_clause->Copy();
	}

	return std::move(copy);
}

void MatchRef::Serialize(FieldWriter &writer) const {
	writer.WriteString(pg_name);
	writer.WriteString(alias);
	writer.WriteSerializableList<PathPattern>(path_list);
	writer.WriteSerializableList<ParsedExpression>(column_list);
	writer.WriteOptional(where_clause);
}

unique_ptr<TableRef> MatchRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<MatchRef>();

	result->pg_name = reader.ReadRequired<string>();
	result->alias = reader.ReadRequired<string>();
	result->path_list = reader.ReadRequiredSerializableList<PathPattern>();
	result->column_list = reader.ReadRequiredSerializableList<ParsedExpression>();
	result->where_clause = reader.ReadOptional<ParsedExpression>(nullptr);
	return result;
}

}