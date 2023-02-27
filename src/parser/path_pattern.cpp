#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/path_pattern.hpp"

namespace duckdb {

PathPattern::PathPattern() {

}

void PathPattern::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteSerializableList<PathElement>(path_elements);
	writer.WriteSerializable<ParsedExpression>(*where_clause);
}

unique_ptr<PathPattern> PathPattern::Deserialize(Deserializer &deserializer) {
	auto result = make_unique<PathPattern>();
	FieldReader reader(deserializer);
	result->path_elements = reader.ReadRequiredSerializableList<PathElement>();
	result->where_clause = reader.ReadRequiredSerializable<ParsedExpression>();
	return unique_ptr<PathPattern>();
}

bool PathPattern::Equals(const PathPattern *other_p) const {
	if (!where_clause->Equals(other_p->where_clause.get())) {
		return false;
	}
	for (idx_t idx = 0; idx < path_elements.size(); idx++) {
		if (!path_elements[idx]->Equals(other_p->path_elements[idx].get())) {
			return false;
		}
	}

	return true;
}

}