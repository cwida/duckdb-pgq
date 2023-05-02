#include "duckdb/parser/subpath_element.hpp"

namespace duckdb {

unique_ptr<PathReference> SubPath::Copy() {
	auto result = make_uniq<SubPath>(PGQPathReferenceType::SUBPATH);

	result->path_reference_type = path_reference_type;
	result->path_mode = path_mode;
	for (auto &path : path_list) {
		result->path_list.push_back(path->Copy());
	}
	if (where_clause) {
		result->where_clause = where_clause->Copy();
	}
	result->lower = lower;
	result->upper = upper;
	result->single_bind = single_bind;

	return std::move(result);
}
bool SubPath::Equals(const PathReference *other_p) const {
	if (!PathReference::Equals(other_p)) {
		return false;
	}
	auto other = (SubPath *)other_p;
	if (path_list.size() != other->path_list.size()) {
		return false;
	}

	for (idx_t i = 0; i < path_list.size(); i++) {
		if (!path_list[i]->Equals(other->path_list[i].get())) {
			return false;
		}
	}

	if (where_clause && other->where_clause.get()) {
		if (!where_clause->Equals(other->where_clause.get())) {
			return false;
		}
	}
	if ((where_clause && !other->where_clause.get()) || (!where_clause && other->where_clause.get())) {
		return false;
	}

	if (path_mode != other->path_mode) {
		return false;
	}
	if (lower != other->lower) {
		return false;
	}
	if (upper != other->upper) {
		return false;
	}
	if (single_bind != other->single_bind) {
		return false;
	}
	return true;
}
void SubPath::Serialize(FieldWriter &writer) const {
	writer.WriteField<PGQPathMode>(path_mode);
	writer.WriteSerializableList(path_list);
	writer.WriteField<bool>(single_bind);
	writer.WriteField<int32_t>(lower);
	writer.WriteField<int32_t>(upper);
	writer.WriteOptional(where_clause);
}

unique_ptr<PathReference> SubPath::Deserialize(FieldReader &reader) {
	auto result = make_uniq<SubPath>(PGQPathReferenceType::SUBPATH);

	result->path_mode = reader.ReadRequired<PGQPathMode>();
	result->path_list = reader.ReadRequiredSerializableList<PathReference>();
	result->single_bind = reader.ReadRequired<bool>();
	result->lower = reader.ReadRequired<int32_t>();
	result->upper = reader.ReadRequired<int32_t>();
	result->where_clause = reader.ReadOptional<ParsedExpression>(nullptr);
	return std::move(result);
}
string SubPath::ToString() const {
	string result = "";
	if (path_list.size() == 1) {
		switch (path_list[0]->path_reference_type) {
		case PGQPathReferenceType::PATH_ELEMENT: {
			auto path_element = reinterpret_cast<PathElement *>(path_list[0].get());
			switch (path_element->match_type) {
			case PGQMatchType::MATCH_VERTEX:
				result += "(" + path_element->variable_binding + ":" + path_element->label +
				          (where_clause ? " WHERE " + where_clause->ToString() : "") + ")";
				break;
			case PGQMatchType::MATCH_EDGE_ANY:
				result += "-[" + path_element->variable_binding + ":" + path_element->label +
				          (where_clause ? " WHERE " + where_clause->ToString() : "") + "]-";
				break;
			case PGQMatchType::MATCH_EDGE_LEFT:
				result += "<-[" + path_element->variable_binding + ":" + path_element->label +
				          (where_clause ? " WHERE " + where_clause->ToString() : "") + "]-";
				break;
			case PGQMatchType::MATCH_EDGE_RIGHT:
				result += "-[" + path_element->variable_binding + ":" + path_element->label +
				          (where_clause ? " WHERE " + where_clause->ToString() : "") + "]->";
				break;
			case PGQMatchType::MATCH_EDGE_LEFT_RIGHT:
				result += "<-[" + path_element->variable_binding + ":" + path_element->label +
				          (where_clause ? " WHERE " + where_clause->ToString() : "") + "]->";
				break;
			}
			break;
		}
		case PGQPathReferenceType::SUBPATH:
			result += " " + path_list[0]->ToString();
			break;
		}
	}
	if (lower != upper) {
		result += "{" + std::to_string(lower) + "," + std::to_string(upper) + "}";
	}
	return result;
}
} // namespace duckdb
