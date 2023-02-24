#include "duckdb/parser/tableref/matchref.hpp"

namespace duckdb {


unique_ptr<TableRef> Transformer::TransformMatch(duckdb_libpgquery::PGMatchClause *root) {

	auto result = make_unique<MatchRef>();

	result->pg_name = root->pg_name; // Name of the property graph to bind to
	result->where_clause = TransformExpression(root->where_clause);

	for (auto node = root->columns->head; node != nullptr; node = lnext(node)) {
		auto column = reinterpret_cast<duckdb_libpgquery::PGList *>(node->data.ptr_value);
		auto column_spec = column->head;
		auto target = reinterpret_cast<duckdb_libpgquery::PGResTarget *>(column->head->next->data.ptr_value);
		auto res_target = TransformResTarget(target);
		result->column_list.emplace_back(res_target);
	}
	return result;
}


} // namespace duckdb