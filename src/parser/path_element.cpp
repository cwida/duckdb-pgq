#include "duckdb/parser/path_element.hpp"

namespace duckdb {

bool PathElement::Equals(const PathElement *other_p) const {
	if (match_type != other_p->match_type) {
		return false;
	}
	if (label != other_p->label) {
		return false;
	}
	if (variable_binding != other_p->variable_binding) {
		return false;
	}
	return true;
}

void PathElement::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(match_type);
	writer.WriteString(label);
	writer.WriteString(variable_binding);
}

unique_ptr<PathElement> PathElement::Deserialize(Deserializer &deserializer) {
	auto result = make_unique<PathElement>();
	FieldReader reader(deserializer);
	result->match_type = reader.ReadRequired<string>();
	result->label = reader.ReadRequired<string>();
	result->variable_binding = reader.ReadRequired<string>();
	return result;
}

}
