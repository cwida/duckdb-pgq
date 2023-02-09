#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/property_graph_table_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformCreatePropertyGraphTable(duckdb_libpgquery::PGPropertyGraphTable *node) {
	int foo __attribute__((unused)) = 0;


//	auto result = make_unique<PropertyGraphTableReference>(node);



}




}