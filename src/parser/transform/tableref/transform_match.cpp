#include "duckdb/parser/tableref/matchref.hpp"

namespace duckdb {

unique_ptr<GraphElementPattern> Transformer::TransformPath(duckdb_libpgquery::PGPathPattern *root) {
	auto result = make_unique<GraphElementPattern>();


	return result;
}


unique_ptr<TableRef> Transformer::TransformMatch(duckdb_libpgquery::PGMatchClause *root) {

	auto result = make_unique<MatchRef>();

	result->pg_name = root->pg_name; // Name of the property graph to bind to
	if (result->where_clause) {
		result->where_clause = TransformExpression(root->where_clause);
	}

	for (auto node = root->paths->head; node != nullptr; node = lnext(node)) {
		auto path = reinterpret_cast<duckdb_libpgquery::PGPathPattern *>(node->data.ptr_value);
		TransformPath(path);
	}

	for (auto node = root->columns->head; node != nullptr; node = lnext(node)) {
		auto column = reinterpret_cast<duckdb_libpgquery::PGList *>(node->data.ptr_value);
		auto column_spec = column->head;
		auto target = reinterpret_cast<duckdb_libpgquery::PGResTarget *>(column->head->next->data.ptr_value);
		auto res_target = TransformResTarget(target);
		result->column_list.push_back(std::move(res_target));
	}
	return result;
}


} // namespace duckdb