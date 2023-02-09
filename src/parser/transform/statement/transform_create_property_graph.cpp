#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreatePropertyGraph(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreatePropertyGraphStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreatePropertyGraphInfo>();

	auto qname = TransformQualifiedName(stmt->name);
	info->property_graph_name = qname.name;

	if (stmt->vertex_tables) {
		vector<unique_ptr<ParsedExpression>> vertex_tables;
		TransformExpressionList(*stmt->vertex_tables, vertex_tables);
		for (auto &vertex_table __attribute__((unused)) : vertex_tables) {
			continue;
		}
	}

//	info->on_conflict = TransformOnCoflict(stmt->onconflict);
//	info->temporary =
//	    stmt->relation->relpersistence == duckdb_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;

//	if (info->temporary && stmt->oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_PRESERVE_ROWS &&
//	    stmt->oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_NOOP) {
//		throw NotImplementedException("Only ON COMMIT PRESERVE ROWS is supported");
//	}
//	if (!stmt->tableElts) {
//		throw ParserException("Table must have at least one column!");
//	}

//	idx_t column_count = 0;
//	for (auto c = stmt->tableElts->head; c != nullptr; c = lnext(c)) {
//		auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(c->data.ptr_value);
//		switch (node->type) {
//		case duckdb_libpgquery::T_PGColumnDef: {
//			auto cdef = (duckdb_libpgquery::PGColumnDef *)c->data.ptr_value;
//			auto centry = TransformColumnDefinition(cdef);
//			if (cdef->constraints) {
//				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
//					auto constraint = TransformConstraint(constr, centry, info->columns.LogicalColumnCount());
//					if (constraint) {
//						info->constraints.push_back(std::move(constraint));
//					}
//				}
//			}
//			info->columns.AddColumn(std::move(centry));
//			column_count++;
//			break;
//		}
//		case duckdb_libpgquery::T_PGConstraint: {
//			info->constraints.push_back(TransformConstraint(c));
//			break;
//		}
//		default:
//			throw NotImplementedException("ColumnDef type not handled yet");
//		}
//	}
//
//	if (!column_count) {
//		throw ParserException("Table must have at least one column!");
//	}

//	result->info = std::move(info);
	return result;
}

} // namespace duckdb
