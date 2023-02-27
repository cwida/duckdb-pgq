#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/path_pattern.hpp"

namespace duckdb {

PathPattern::PathPattern() {

}

void PathPattern::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteSerializableList(path_elements);
	writer.WriteOptional(where_clause);
	writer.Finalize();
}

unique_ptr<PathPattern> PathPattern::Deserialize(Deserializer &deserializer) {
	auto result = make_unique<PathPattern>();
	FieldReader reader(deserializer);
	result->path_elements = reader.ReadRequiredSerializableList<PathElement>();
	result->where_clause = reader.ReadOptional<ParsedExpression>(nullptr);
	reader.Finalize();
	return result;
}

bool PathPattern::Equals(const PathPattern *other_p) const {
	if (where_clause && other_p->where_clause.get()) {
		if (!where_clause->Equals(other_p->where_clause.get())) {
			return false;
		}
	}
	if ((where_clause && !other_p->where_clause.get())
	    || (!where_clause && other_p->where_clause.get())) {
		return false;
	}
	for (idx_t idx = 0; idx < path_elements.size(); idx++) {
		if (!path_elements[idx]->Equals(other_p->path_elements[idx].get())) {
			return false;
		}
	}

	return true;
}
unique_ptr<PathPattern> PathPattern::Copy() {
	auto result = make_unique<PathPattern>();

	for (auto &path_element: path_elements) {
		result->path_elements.push_back(path_element->Copy());
	}

	if (result->where_clause) {
		result->where_clause = where_clause->Copy();
	}

	return result;
}

}