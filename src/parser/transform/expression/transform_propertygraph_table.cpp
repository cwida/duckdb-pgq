#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/property_graph_table_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

#if 0


typedef struct PGPropertyGraphTable {
	PGNodeTag type;

	/* Fields used for both edge and vertex table */
	PGRangeVar *table; /* name of a SQL table */
	PGRangeVar *name; /* name the element (default=table name)*/

	PGList *properties; /* list of column names */
	PGList *labels; /* last label is main label, other labels depend on bit in discriminator (if present) */
	PGRangeVar *discriminator; /* if non-NULL: BIGINT column ident with 64 bits for max 64 sub-label membership */

	bool is_vertex_table;

	/* Fields only used for Edge Tables */
	PGList *src_fk, *dst_fk;
	PGList *src_pk, *dst_pk;
	PGRangeVar *src_name, *dst_name;
} PGPropertyGraphTable;
#endif

unique_ptr<ParsedExpression> Transformer::TransformCreatePropertyGraphTable(duckdb_libpgquery::PGPropertyGraphTable *node) {
	int foo __attribute__((unused)) = 0;


	auto fields = node->properties;
	vector<string> column_names;
	for (auto node = fields->head; node; lnext(node)) {
		column_names.emplace_back(reinterpret_cast<duckdb_libpgquery::PGValue *>(node->data.ptr_value)->val.str);
	}
	auto colref = make_unique<PropertyGraphTableExpression>(std::move(column_names));


//	auto result = make_unique<PropertyGraphTableReference>(node);



}




}