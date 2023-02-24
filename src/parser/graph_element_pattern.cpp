#include "duckdb/parser/graph_element_pattern.hpp"
#include "duckdb/common/field_writer.hpp"


namespace duckdb {

GraphElementPattern::GraphElementPattern() {

}

void GraphElementPattern::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteSerializable<ParsedExpression>(*where_clause);
}

unique_ptr<GraphElementPattern> GraphElementPattern::Deserialize(Deserializer &deserializer) {
	auto result = make_unique<GraphElementPattern>();
	FieldReader reader(deserializer);
	result->where_clause = reader.ReadRequiredSerializable<ParsedExpression>();
	return unique_ptr<GraphElementPattern>();
}

bool GraphElementPattern::Equals(const GraphElementPattern *other_p) const {
	if (!where_clause->Equals(other_p->where_clause.get())) {
		return false;
	}

	return true;
}

}