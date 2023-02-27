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
	writer.WriteField<PGQMatchType>(match_type);
	writer.WriteString(label);
	writer.WriteString(variable_binding);
}

unique_ptr<PathElement> PathElement::Deserialize(Deserializer &deserializer) {
	auto result = make_unique<PathElement>();
	FieldReader reader(deserializer);
	result->match_type = reader.ReadRequired<PGQMatchType>();
	result->label = reader.ReadRequired<string>();
	result->variable_binding = reader.ReadRequired<string>();
	return result;
}
unique_ptr<PathElement> PathElement::Copy() {
	auto result = make_unique<PathElement>();
	result->match_type = match_type;
	result->label = label;
	result->variable_binding = variable_binding;

	return result;
}

}
