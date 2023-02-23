#include "duckdb/parser/tableref/matchref.hpp"

namespace duckdb {


unique_ptr<TableRef> Transformer::TransformMatch(duckdb_libpgquery::PGMatchClause *root) {

	auto result = unique_ptr<MatchRef>();

	result->pg_name = root->pg_name; // Name of the property graph to bind to



	return result;
}


} // namespace duckdb