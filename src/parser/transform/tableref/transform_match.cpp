#include "duckdb/parser/tableref/matchref.hpp"

namespace duckdb {

unique_ptr<PathElement> Transformer::TransformPathElement(duckdb_libpgquery::PGPathElement *element) {
	//! Vertex or edge pattern
	auto result = make_unique<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	switch(element->match_type) {
	case duckdb_libpgquery::PG_MATCH_VERTEX:
		result->match_type = PGQMatchType::MATCH_VERTEX;
		break;
	case duckdb_libpgquery::PG_MATCH_EDGE_ANY:
		result->match_type = PGQMatchType::MATCH_EDGE_ANY;
		break;
	case duckdb_libpgquery::PG_MATCH_EDGE_LEFT:
		result->match_type = PGQMatchType::MATCH_EDGE_LEFT;
		break;
	case duckdb_libpgquery::PG_MATCH_EDGE_RIGHT:
		result->match_type = PGQMatchType::MATCH_EDGE_RIGHT;
		break;
	case duckdb_libpgquery::PG_MATCH_EDGE_LEFT_RIGHT:
		result->match_type = PGQMatchType::MATCH_EDGE_LEFT_RIGHT;
		break;
	default:
		throw InternalException("Unrecognized match type detected");
	}
	auto label_expression = reinterpret_cast<duckdb_libpgquery::PGLabelTest *>(element->label_expr);
	result->label = label_expression->name;
	result->variable_binding = element->element_var;
	return result;
}

unique_ptr<SubPath> Transformer::TransformSubPathElement(duckdb_libpgquery::PGSubPath *element) {
	auto result = make_unique<SubPath>(PGQPathReferenceType::SUBPATH);


	return result;
}


unique_ptr<PathPattern> Transformer::TransformPath(duckdb_libpgquery::PGPathPattern *root) {
	auto result = make_unique<PathPattern>();

	//! Path sequence
	for (auto node = root->path->head; node != nullptr; node = lnext(node)) {
		//Parse path element
		auto path_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		if (path_node->type == duckdb_libpgquery::T_PGPathElement) {
			auto element = reinterpret_cast<duckdb_libpgquery::PGPathElement *>(path_node);
			auto path_element = TransformPathElement(element);
			result->path_elements.push_back(std::move(path_element));
		} else if (path_node->type == duckdb_libpgquery::T_PGSubPath) {
			auto subpath = reinterpret_cast<duckdb_libpgquery::PGSubPath *>(path_node);
			auto subpath_element = TransformSubPathElement(subpath);
			result->path_elements.push_back(std::move(subpath_element));
		}
	}

	return result;
}


unique_ptr<TableRef> Transformer::TransformMatch(duckdb_libpgquery::PGMatchClause *root) {

	auto result = make_unique<MatchRef>();

	result->pg_name = root->pg_name; // Name of the property graph to bind to

	auto alias = TransformQualifiedName(root->graph_table);
	result->alias = alias.name;

	if (root->where_clause) {
		result->where_clause = TransformExpression(root->where_clause);
	}

	for (auto node = root->paths->head; node != nullptr; node = lnext(node)) {
		auto path = reinterpret_cast<duckdb_libpgquery::PGPathPattern *>(node->data.ptr_value);
		auto transformed_path = TransformPath(path);
		result->path_list.push_back(std::move(transformed_path));
	}

	for (auto node = root->columns->head; node != nullptr; node = lnext(node)) {
		auto column = reinterpret_cast<duckdb_libpgquery::PGList *>(node->data.ptr_value);
		auto target = reinterpret_cast<duckdb_libpgquery::PGResTarget *>(column->head->next->data.ptr_value);
		auto res_target = TransformResTarget(target);
		result->column_list.push_back(std::move(res_target));
	}
	return result;
}


} // namespace duckdb