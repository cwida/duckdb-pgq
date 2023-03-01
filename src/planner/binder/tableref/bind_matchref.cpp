#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/tableref/matchref.hpp"


namespace duckdb {

shared_ptr<PropertyGraphTable> FindGraphTable(unique_ptr<PathElement> &path_element, CreatePropertyGraphInfo &pg_table) {

	auto graph_table_entry = pg_table.label_map.find(path_element->label);
	if (graph_table_entry == pg_table.label_map.end()) {
		throw BinderException("The label %s is not registered in property graph %s", path_element->label, pg_table.property_graph_name);
	}

	return graph_table_entry->second;
}

void CheckEdgeTableConstraints(const string& src_reference, const string& dst_reference, shared_ptr<PropertyGraphTable> &edge_table) {
	if (src_reference != edge_table->source_reference) {
		throw BinderException("Label %s is not registered as a source reference for edge pattern of table %s", src_reference, edge_table->table_name);
	}
	if (dst_reference != edge_table->destination_reference) {
		throw BinderException("Label %s is not registered as a destination reference for edge pattern of table %s", src_reference, edge_table->table_name);
	}
}

unique_ptr<ParsedExpression> CreateMatchJoinExpression(vector<string> vertex_keys, vector<string> edge_keys,
                                                       const string &vertex_alias, const string &edge_alias) {
	vector<unique_ptr<ParsedExpression>> conditions;

	if (vertex_keys.size() != edge_keys.size()) {
		throw BinderException("Vertex columns and edge columns size mismatch");
	}
	for (idx_t i = 0; i < vertex_keys.size(); i++) {
		auto vertex_colref = make_unique<ColumnRefExpression>(vertex_keys[i], vertex_alias);
		auto edge_colref = make_unique<ColumnRefExpression>(edge_keys[i], edge_alias);
		conditions.push_back(make_unique<ComparisonExpression>(
		    ExpressionType::COMPARE_EQUAL, std::move(vertex_colref), std::move(edge_colref)));
	}
	unique_ptr<ParsedExpression> where_clause;

	for (auto &condition : conditions) {
		if (where_clause) {
			where_clause = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(where_clause), std::move(condition));
		} else {
			where_clause = std::move(condition);
		}
	}

	return where_clause;
}


unique_ptr<BoundTableRef> Binder::Bind(MatchRef &ref) {
	auto &client_data = ClientData::Get(context);

	auto pg_table_entry = client_data.registered_property_graphs.find(ref.pg_name);
	if (pg_table_entry == client_data.registered_property_graphs.end()) {
		throw BinderException("Property graph %s does not exist", ref.pg_name);
	}

	auto pg_table = reinterpret_cast<CreatePropertyGraphInfo *>(pg_table_entry->second.get());

	vector<unique_ptr<ParsedExpression>> conditions;
	conditions.push_back(std::move(ref.where_clause));

	auto select_node = make_unique<SelectNode>();
	auto subquery = make_unique<SelectStatement>();
	unordered_map<string, string> alias_map;

	for (idx_t idx_i = 0; idx_i < ref.path_list.size(); idx_i++) {
		auto &path_list = ref.path_list[idx_i];
		if (path_list->path_elements.size() % 2 == 0) {
			throw BinderException("Even number of element patterns is not supported.");
		}

		auto &previous_vertex_element = path_list->path_elements[0];
		auto previous_vertex_table = FindGraphTable(previous_vertex_element, *pg_table);

		alias_map[previous_vertex_element->variable_binding] = previous_vertex_table->table_name;
		if (path_list->path_elements.size() == 1) {
			auto from_clause = make_unique<BaseTableRef>();
			auto graph_table = FindGraphTable(previous_vertex_element, *pg_table);
			from_clause->table_name = graph_table->table_name;
			from_clause->alias = previous_vertex_element->variable_binding;
			select_node->from_table = std::move(from_clause);
		} else {
			for (idx_t idx_j = 1; idx_j < ref.path_list[idx_i]->path_elements.size(); idx_j = idx_j + 2) {
				auto &edge_element = ref.path_list[idx_i]->path_elements[idx_j];
				auto &next_vertex_element = path_list->path_elements[idx_j + 1];
				if (next_vertex_element->match_type != PGQMatchType::MATCH_VERTEX ||
				    previous_vertex_element->match_type != PGQMatchType::MATCH_VERTEX) {
					throw BinderException("Vertex and edge patterns must be alternated.");
				}

				auto next_vertex_table = FindGraphTable(next_vertex_element, *pg_table);
				auto edge_table = FindGraphTable(edge_element, *pg_table);

				// check aliases
				alias_map[next_vertex_element->variable_binding] = next_vertex_table->table_name;
				alias_map[edge_element->variable_binding] = edge_table->table_name;

				switch(edge_element->match_type) {
				case PGQMatchType::MATCH_EDGE_ANY:
					throw NotImplementedException("The match statement contains a undirected edge pattern which is not yet implemented.");
				case PGQMatchType::MATCH_EDGE_LEFT:
					CheckEdgeTableConstraints(next_vertex_table->table_name, previous_vertex_table->table_name, edge_table);
					break;
				case PGQMatchType::MATCH_EDGE_RIGHT:
					CheckEdgeTableConstraints(previous_vertex_table->table_name, next_vertex_table->table_name, edge_table);
					conditions.push_back(CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
					                                               previous_vertex_element->variable_binding, edge_element->variable_binding));
					conditions.push_back(CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
					                                               next_vertex_element->variable_binding, edge_element->variable_binding));
					previous_vertex_element = std::move(next_vertex_element);
					break;
				case PGQMatchType::MATCH_EDGE_LEFT_RIGHT:
					break;
				default:
					throw InternalException("Unknown match type found");
				}
				// Check the edge type
				// If (a)-[b]->(c) 	-> 	b.src = a.id AND b.dst = c.id
				// If (a)<-[b]-(c) 	-> 	b.dst = a.id AND b.src = c.id
				// If (a)-[b]-(c)  	-> 	(b.src = a.id AND b.dst = c.id) OR
				// 						(b.dst = a.id AND b.src = c.id)
				// If (a)<-[b]->(c)	->  (b.src = a.id AND b.dst = c.id) AND
				//						(b.dst = a.id AND b.src = c.id)
			}
		}
	}

	unique_ptr<TableRef> from_clause;

	for (auto &table_alias_entry : alias_map) {
		auto table_ref = make_unique<BaseTableRef>();
		table_ref->table_name = table_alias_entry.second;
		table_ref->alias = table_alias_entry.first;

		if (from_clause) {
			auto new_root = make_unique<JoinRef>(JoinRefType::CROSS);
			new_root->left = std::move(from_clause);
			new_root->right = std::move(table_ref);
			from_clause = std::move(new_root);
		} else {
			from_clause = std::move(table_ref);
		}
	}
	select_node->from_table = std::move(from_clause);

	unique_ptr<ParsedExpression> where_clause;
	for (auto &condition : conditions) {
		if (where_clause) {
			where_clause = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
			                                                  std::move(where_clause), std::move(condition));
		} else {
			where_clause = std::move(condition);
		}
	}
	select_node->where_clause = std::move(where_clause);

	select_node->select_list = std::move(ref.column_list);
	subquery->node = std::move(select_node);

	auto result = make_unique<SubqueryRef>(std::move(subquery), ref.alias);

	return Bind(*result);
}
} // namespace duckdb

