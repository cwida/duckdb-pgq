#include "duckdb/common/exception.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformCreatePropertyGraphTable(duckdb_libpgquery::PGPropertyGraphTable *node) {

	auto result = make_unique<PropertyGraphTableReference>(node->position);



}




}