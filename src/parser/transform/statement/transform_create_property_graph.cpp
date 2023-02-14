#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<PropertyGraphTable> Transformer::TransformPropertyGraphTable(duckdb_libpgquery::PGPropertyGraphTable *graph_table) {
	vector<string> column_names;
	vector<string> label_names;

	auto graph_table_name = TransformQualifiedName(graph_table->table);

	// TODO
	//  	- check if properties is null, in that case all columns from the table are properties
	// 		- check if properties has an except list
	// 		- all columns

	for (auto property_element = graph_table->properties->head; property_element != nullptr;
	     property_element = property_element->next) {
		auto column_optional_as = reinterpret_cast<duckdb_libpgquery::PGList *>(property_element->data.ptr_value);
		auto column_name = reinterpret_cast<duckdb_libpgquery::PGColumnDef *>(column_optional_as->head->data.ptr_value);
		auto column_alias = reinterpret_cast<duckdb_libpgquery::PGColumnDef *>(column_optional_as->head->next->data.ptr_value);
		// TODO
		//  	- 	Change this to support the optional as
		// 		  	Looking at the next element of column_optional_as, which is a linked list
		// 			If the string is equal to the first string then there is no alias
		column_names.emplace_back(column_name->colname);
	}

	for (auto label_element = graph_table->labels->head; label_element != nullptr; label_element = label_element->next) {
		auto label = reinterpret_cast<duckdb_libpgquery::PGValue *>(label_element->data.ptr_value);
		D_ASSERT(label->type == duckdb_libpgquery::T_PGString);
		// TODO
		//		- Make sure labels are unique within a LabelList
		//			Probably easiest is to convert to a set and see if the length is not equal
		//			Other option is having a map (set) and keeping track of the entries in that set
		label_names.emplace_back(label->val.str);
	}

	unique_ptr<PropertyGraphTable> pg_table = make_unique<PropertyGraphTable>(graph_table_name.name, column_names, label_names);

	pg_table->is_vertex_table = graph_table->is_vertex_table;

	if (graph_table->discriminator) {
		//! In this case there is a list with length > 1 of labels
		//! of which the last element in the list is the main label
		auto discriminator = TransformQualifiedName(graph_table->discriminator);
		pg_table->discriminator = discriminator.name;
	}

	//! Everything from this point is only related to edge tables

	if (!graph_table->is_vertex_table) {
		D_ASSERT(graph_table->src_name);
		auto src_name = TransformQualifiedName(graph_table->src_name);
		pg_table->source_reference = src_name.name;

		D_ASSERT(graph_table->dst_name);
		auto dst_name = TransformQualifiedName(graph_table->dst_name);
		pg_table->destination_reference = dst_name.name;

		for (auto &src_key = graph_table->src_pk->head; src_key != nullptr; src_key = lnext(src_key)) {
			auto key = reinterpret_cast<duckdb_libpgquery::PGValue *>(src_key->data.ptr_value);
			pg_table->source_pk.emplace_back(key->val.str);
		}

		for (auto &dst_key = graph_table->dst_pk->head; dst_key != nullptr; dst_key = lnext(dst_key)) {
			auto key = reinterpret_cast<duckdb_libpgquery::PGValue *>(dst_key->data.ptr_value);
			pg_table->destination_pk.emplace_back(key->val.str);
		}

		for (auto &src_key = graph_table->src_fk->head; src_key != nullptr; src_key = lnext(src_key)) {
			auto key = reinterpret_cast<duckdb_libpgquery::PGValue *>(src_key->data.ptr_value);
			pg_table->source_fk.emplace_back(key->val.str);
		}

		for (auto &dst_key = graph_table->dst_fk->head; dst_key != nullptr; dst_key = lnext(dst_key)) {
			auto key = reinterpret_cast<duckdb_libpgquery::PGValue *>(dst_key->data.ptr_value);
			pg_table->destination_fk.emplace_back(key->val.str);
		}


	}

	return pg_table;

}

unique_ptr<CreateStatement> Transformer::TransformCreatePropertyGraph(duckdb_libpgquery::PGNode *root) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreatePropertyGraphStmt *>(root);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreatePropertyGraphInfo>();

	auto property_graph_name = TransformQualifiedName(stmt->name);
	info->property_graph_name = property_graph_name.name;

	D_ASSERT(stmt->vertex_tables);
	for (auto &vertex_table = stmt->vertex_tables->head; vertex_table != nullptr; vertex_table = lnext(vertex_table)) {
		auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(vertex_table->data.ptr_value);

		if (node->type != duckdb_libpgquery::T_PGPropertyGraphTable) {
			throw NotImplementedException("CreatePropertyGraphTable not implemented.");
		}
		auto graph_table = reinterpret_cast<duckdb_libpgquery::PGPropertyGraphTable *>(vertex_table->data.ptr_value);
		auto pg_table = TransformPropertyGraphTable(graph_table);
		info->graph_tables.push_back(std::move(pg_table));

	}

	if (stmt->edge_tables) {
		for (auto &edge_table = stmt->edge_tables->head; edge_table != nullptr; edge_table = lnext(edge_table)) {
			auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(edge_table->data.ptr_value);

			if (node->type != duckdb_libpgquery::T_PGPropertyGraphTable) {
				throw NotImplementedException("CreatePropertyGraphTable not implemented.");
			}
			auto graph_table = reinterpret_cast<duckdb_libpgquery::PGPropertyGraphTable *>(edge_table->data.ptr_value);
			auto pg_table = TransformPropertyGraphTable(graph_table);
			info->graph_tables.push_back(std::move(pg_table));
		}
	}

	result->info = std::move(info);

	return result;

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
