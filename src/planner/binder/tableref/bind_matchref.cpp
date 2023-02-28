#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/tableref/matchref.hpp"

namespace duckdb {
unique_ptr<BoundTableRef> Binder::Bind(MatchRef &ref) {
	auto &client_data = ClientData::Get(context);

	auto pg_table_entry = client_data.registered_property_graphs.find(ref.pg_name);
	if (pg_table_entry == client_data.registered_property_graphs.end()) {
		throw BinderException("Property graph %s does not exist", ref.pg_name);
	}

	auto pg_table = reinterpret_cast<CreatePropertyGraphInfo *>(pg_table_entry->second.get());


	auto select_node = make_unique<SelectNode>();
	auto from_clause = make_unique<BaseTableRef>();
	auto subquery = make_unique<SelectStatement>();

	for (idx_t idx_i = 0; idx_i < ref.path_list.size(); idx_i++) {
		for (idx_t idx_j = 0; idx_j < ref.path_list[idx_i]->path_elements.size(); idx_j++) {
			// check if the label exists in the property graph
			auto &path_element = ref.path_list[idx_i]->path_elements[idx_j];
			auto graph_table_entry = pg_table->label_map.find(path_element->label);
			if (graph_table_entry == pg_table->label_map.end()) {
				throw BinderException("The label %s is not registered as a vertex or edge table in property graph %s", path_element->label, pg_table->property_graph_name);
			}
			from_clause->table_name = graph_table_entry->second->table_name;
			from_clause->alias = path_element->variable_binding;
		}
	}

	// TODO handle where clauses here

	select_node->select_list = std::move(ref.column_list);
	select_node->from_table = std::move(from_clause);

	select_node->where_clause = std::move(ref.where_clause);

	subquery->node = std::move(select_node);

	auto result = make_unique<SubqueryRef>(std::move(subquery), ref.alias);

	return Bind(*result);
}
} // namespace duckdb

