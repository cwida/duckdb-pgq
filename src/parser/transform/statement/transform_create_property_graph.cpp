#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<PropertyGraphTable> Transformer::TransformPropertyGraphTable(duckdb_libpgquery::PGPropertyGraphTable *graph_table) {
	vector<string> column_names;
	vector<string> label_names;

	auto graph_table_name = TransformQualifiedName(graph_table->table);

	// TODO
	//  	- check if properties is null
	// 		- check if properties has an except list
	// 		- all columns

	for (auto properties_list = graph_table->properties->head;
	     properties_list != nullptr;
	     properties_list = properties_list->next) {
		auto column_optional_as = reinterpret_cast<duckdb_libpgquery::PGList *>(properties_list->data.ptr_value);
		auto cdef = reinterpret_cast<duckdb_libpgquery::PGColumnDef *>(column_optional_as->head->data.ptr_value);
		// TODO
		//  	- 	Change this to support the optional as
		// 		  	Looking at the next element of column_optional_as, which is a linked list
		// 			If the string is equal to the first string then there is no alias
		column_names.push_back(cdef->colname);
	}




	unique_ptr<PropertyGraphTable> pg_table = make_unique<PropertyGraphTable>(column_names, label_names);

	return pg_table;

}

unique_ptr<CreateStatement> Transformer::TransformCreatePropertyGraph(duckdb_libpgquery::PGNode *root) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreatePropertyGraphStmt *>(root);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreatePropertyGraphInfo>();

	auto property_graph_name = TransformQualifiedName(stmt->name);
	info->property_graph_name = property_graph_name.name;

	if (stmt->vertex_tables) {
		vector<unique_ptr<ParsedExpression>> vertex_tables;
		for (auto &vertex_table = stmt->vertex_tables->head; vertex_table != nullptr; vertex_table = lnext(vertex_table)) {
			auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(vertex_table->data.ptr_value);

			switch (node->type) {
			case duckdb_libpgquery::T_PGPropertyGraphTable: {
				auto graph_table = reinterpret_cast<duckdb_libpgquery::PGPropertyGraphTable *>(vertex_table->data.ptr_value);
				auto graph_table_name = TransformQualifiedName(graph_table->table);
				auto pg_table = TransformPropertyGraphTable(graph_table);


			}
			default:
				throw NotImplementedException("CreatePropertyGraphTable not implemented.");


			}
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
