
#pragma once

namespace duckdb {

class PathElement {
public:
	std::string match_type; // probably better enum

	std::string label;

	std::string variable_binding;

public:
	bool Equals(const PathElement *other_p) const;

	void Serialize(Serializer &serializer) const;

	static unique_ptr<PathElement> Deserialize(Deserializer &deserializer);
};
}
