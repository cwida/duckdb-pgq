#include "duckdb/parser/graph_element_pattern.hpp"

#include "duckdb/common/field_writer.hpp"


namespace duckdb {

GraphElementPattern::GraphElementPattern() {

}

void GraphElementPattern::Serialize(Serializer &serializer) const {
}
unique_ptr<GraphElementPattern> GraphElementPattern::Deserialize(Deserializer &deserializer) {
	return unique_ptr<GraphElementPattern>();
}

}