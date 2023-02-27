
#pragma once

namespace duckdb {

enum class PGQMatchType : uint8_t {
	MATCH_VERTEX = 0,
	MATCH_EDGE_ANY = 1,
	MATCH_EDGE_LEFT = 2,
	MATCH_EDGE_RIGHT = 3,
	MATCH_EDGE_LEFT_RIGHT = 4
};

class PathElement {
public:
	PGQMatchType match_type; // probably better enum

	std::string label;

	std::string variable_binding;

public:
	bool Equals(const PathElement *other_p) const;

	void Serialize(Serializer &serializer) const;

	static unique_ptr<PathElement> Deserialize(Deserializer &deserializer);
};
}
