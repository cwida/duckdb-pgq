#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/tableref/matchref.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

namespace duckdb {

shared_ptr<PropertyGraphTable> FindGraphTable(const string &label, CreatePropertyGraphInfo &pg_table) {
	auto graph_table_entry = pg_table.label_map.find(label);
	if (graph_table_entry == pg_table.label_map.end()) {
		throw BinderException("The label %s is not registered in property graph %s", label,
		                      pg_table.property_graph_name);
	}

	return graph_table_entry->second;
}

void CheckEdgeTableConstraints(const string &src_reference, const string &dst_reference,
                               shared_ptr<PropertyGraphTable> &edge_table) {
	if (src_reference != edge_table->source_reference) {
		throw BinderException("Label %s is not registered as a source reference for edge pattern of table %s",
		                      src_reference, edge_table->table_name);
	}
	if (dst_reference != edge_table->destination_reference) {
		throw BinderException("Label %s is not registered as a destination reference for edge pattern of table %s",
		                      src_reference, edge_table->table_name);
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
		conditions.push_back(make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(vertex_colref),
		                                                       std::move(edge_colref)));
	}
	unique_ptr<ParsedExpression> where_clause;

	for (auto &condition : conditions) {
		if (where_clause) {
			where_clause = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(where_clause),
			                                                  std::move(condition));
		} else {
			where_clause = std::move(condition);
		}
	}

	return where_clause;
}

PathElement *GetPathElement(unique_ptr<PathReference> &path_reference,
                            vector<unique_ptr<ParsedExpression>> &conditions) {
	if (path_reference->path_reference_type == PGQPathReferenceType::PATH_ELEMENT) {
		return reinterpret_cast<PathElement *>(path_reference.get());
	} else if (path_reference->path_reference_type == PGQPathReferenceType::SUBPATH) {
		auto subpath = reinterpret_cast<SubPath *>(path_reference.get());
		if (subpath->where_clause) {
			conditions.push_back(std::move(subpath->where_clause));
		}
		return reinterpret_cast<PathElement *>(subpath->path_list[0].get());
	} else {
		throw InternalException("Unknown path reference type detected");
	}
}

static unique_ptr<SelectStatement> GetCountTable(shared_ptr<PropertyGraphTable> &edge_table,
                                                 const string &prev_binding) {
	auto select_count = make_unique<SelectStatement>();
	auto select_inner = make_unique<SelectNode>();
	auto ref = make_unique<BaseTableRef>();

	ref->table_name = edge_table->source_reference;
	ref->alias = prev_binding;
	select_inner->from_table = std::move(ref);
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ColumnRefExpression>(edge_table->source_pk[0], prev_binding));

	auto count_function = make_unique<FunctionExpression>("count", std::move(children));
	select_inner->select_list.push_back(std::move(count_function));
	select_count->node = std::move(select_inner);
	return select_count;
}

static unique_ptr<JoinRef> GetJoinRef(shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
                                      const string &prev_binding, const string &next_binding) {
	auto first_join_ref = make_unique<JoinRef>(JoinRefType::REGULAR);
	first_join_ref->type = JoinType::INNER;

	auto second_join_ref = make_unique<JoinRef>(JoinRefType::REGULAR);
	second_join_ref->type = JoinType::INNER;

	auto edge_base_ref = make_unique<BaseTableRef>();
	edge_base_ref->table_name = edge_table->table_name;
	edge_base_ref->alias = edge_binding;
	auto src_base_ref = make_unique<BaseTableRef>();
	src_base_ref->table_name = edge_table->source_reference;
	src_base_ref->alias = prev_binding;
	second_join_ref->left = std::move(edge_base_ref);
	second_join_ref->right = std::move(src_base_ref);
	auto t_from_ref = make_unique<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
	auto src_cid_ref = make_unique<ColumnRefExpression>(edge_table->source_pk[0], prev_binding);
	second_join_ref->condition =
	    make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(t_from_ref), std::move(src_cid_ref));
	auto dst_base_ref = make_unique<BaseTableRef>();
	dst_base_ref->table_name = edge_table->destination_reference;
	dst_base_ref->alias = next_binding;
	first_join_ref->left = std::move(second_join_ref);
	first_join_ref->right = std::move(dst_base_ref);

	auto t_to_ref = make_unique<ColumnRefExpression>(edge_table->destination_fk[0], edge_binding);
	auto dst_cid_ref = make_unique<ColumnRefExpression>(edge_table->destination_pk[0], next_binding);
	first_join_ref->condition =
	    make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(t_to_ref), std::move(dst_cid_ref));
	return first_join_ref;
}

unique_ptr<BoundTableRef> Binder::Bind(MatchRef &ref) {
	auto &client_data = ClientData::Get(context);

	auto pg_table_entry = client_data.registered_property_graphs.find(ref.pg_name);
	if (pg_table_entry == client_data.registered_property_graphs.end()) {
		throw BinderException("Property graph %s does not exist", ref.pg_name);
	}

	auto pg_table = reinterpret_cast<CreatePropertyGraphInfo *>(pg_table_entry->second.get());

	auto outer_select_statement = make_unique<SelectStatement>();
	auto cte_select_statement = make_unique<SelectStatement>();

	vector<unique_ptr<ParsedExpression>> conditions;
	conditions.push_back(std::move(ref.where_clause));

	auto select_node = make_unique<SelectNode>();
	auto subquery = make_unique<SelectStatement>();
	unordered_map<string, string> alias_map;

	auto extra_alias_counter = 0;
	bool path_finding = false;
	for (idx_t idx_i = 0; idx_i < ref.path_list.size(); idx_i++) {
		auto &path_list = ref.path_list[idx_i];

		PathElement *previous_vertex_element = GetPathElement(path_list->path_elements[0], conditions);

		auto previous_vertex_table = FindGraphTable(previous_vertex_element->label, *pg_table);
		alias_map[previous_vertex_element->variable_binding] = previous_vertex_table->table_name;

		for (idx_t idx_j = 1; idx_j < ref.path_list[idx_i]->path_elements.size(); idx_j = idx_j + 2) {

			PathElement *edge_element = GetPathElement(path_list->path_elements[idx_j], conditions);
			PathElement *next_vertex_element = GetPathElement(path_list->path_elements[idx_j + 1], conditions);
			if (next_vertex_element->match_type != PGQMatchType::MATCH_VERTEX ||
			    previous_vertex_element->match_type != PGQMatchType::MATCH_VERTEX) {
				throw BinderException("Vertex and edge patterns must be alternated.");
			}

			auto edge_table = FindGraphTable(edge_element->label, *pg_table);
			auto next_vertex_table = FindGraphTable(next_vertex_element->label, *pg_table);

			if (path_list->path_elements[idx_j]->path_reference_type == PGQPathReferenceType::SUBPATH) {
				SubPath *subpath = reinterpret_cast<SubPath *>(path_list->path_elements[idx_j].get());

				if (subpath->upper > 1) {
					path_finding = true;
					auto csr_edge_id_constant = make_unique<ConstantExpression>(Value::INTEGER((int32_t)0));
					auto count_create_edge_select = make_unique<SubqueryExpression>();

					count_create_edge_select->subquery =
					    GetCountTable(edge_table, previous_vertex_element->variable_binding);
					count_create_edge_select->subquery_type = SubqueryType::SCALAR;

					auto cast_subquery_expr = make_unique<SubqueryExpression>();
					auto cast_select_node = make_unique<SelectNode>();

					vector<unique_ptr<ParsedExpression>> csr_vertex_children;
					csr_vertex_children.push_back(make_unique<ConstantExpression>(Value::INTEGER((int32_t)0)));

					auto count_create_vertex_expr = make_unique<SubqueryExpression>();
					count_create_vertex_expr->subquery =
					    GetCountTable(edge_table, previous_vertex_element->variable_binding);
					count_create_vertex_expr->subquery_type = SubqueryType::SCALAR;
					csr_vertex_children.push_back(std::move(count_create_vertex_expr));

					csr_vertex_children.push_back(make_unique<ColumnRefExpression>("dense_id", "sub"));
					csr_vertex_children.push_back(make_unique<ColumnRefExpression>("cnt", "sub"));

					auto create_vertex_function =
					    make_unique<FunctionExpression>("create_csr_vertex", std::move(csr_vertex_children));
					vector<unique_ptr<ParsedExpression>> sum_children;
					sum_children.push_back(std::move(create_vertex_function));
					auto sum_function = make_unique<FunctionExpression>("sum", std::move(sum_children));

					auto inner_select_statement = make_unique<SelectStatement>();
					auto inner_select_node = make_unique<SelectNode>();

					auto source_rowid_colref =
					    make_unique<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
					source_rowid_colref->alias = "dense_id";

					auto count_create_inner_expr = make_unique<SubqueryExpression>();
					count_create_inner_expr->subquery_type = SubqueryType::SCALAR;
					auto edge_src_colref =
					    make_unique<ColumnRefExpression>(edge_table->source_fk[0], edge_element->variable_binding);
					vector<unique_ptr<ParsedExpression>> inner_count_children;
					inner_count_children.push_back(std::move(edge_src_colref));
					auto inner_count_function =
					    make_unique<FunctionExpression>("count", std::move(inner_count_children));
					inner_count_function->alias = "cnt";

					inner_select_node->select_list.push_back(std::move(source_rowid_colref));
					inner_select_node->select_list.push_back(std::move(inner_count_function));
					auto source_rowid_colref_1 =
					    make_unique<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
					expression_map_t<idx_t> grouping_expression_map;
					inner_select_node->groups.group_expressions.push_back(std::move(source_rowid_colref_1));
					GroupingSet grouping_set = {0};
					inner_select_node->groups.grouping_sets.push_back(grouping_set);

					auto inner_join_ref = make_unique<JoinRef>(JoinRefType::REGULAR);
					inner_join_ref->type = JoinType::LEFT;
					auto left_base_ref = make_unique<BaseTableRef>();
					left_base_ref->table_name = edge_table->source_reference;
					left_base_ref->alias = previous_vertex_element->variable_binding;
					auto right_base_ref = make_unique<BaseTableRef>();
					right_base_ref->table_name = edge_table->table_name;
					right_base_ref->alias = edge_element->variable_binding;
					inner_join_ref->left = std::move(left_base_ref);
					inner_join_ref->right = std::move(right_base_ref);

					auto edge_join_colref =
					    make_unique<ColumnRefExpression>(edge_table->source_fk[0], edge_element->variable_binding);
					auto vertex_join_colref = make_unique<ColumnRefExpression>(
					    edge_table->source_pk[0], previous_vertex_element->variable_binding);

					inner_join_ref->condition = make_unique<ComparisonExpression>(
					    ExpressionType::COMPARE_EQUAL, std::move(edge_join_colref), std::move(vertex_join_colref));
					inner_select_node->from_table = std::move(inner_join_ref);
					inner_select_statement->node = std::move(inner_select_node);

					auto inner_from_subquery = make_unique<SubqueryRef>(std::move(inner_select_statement), "sub");

					cast_select_node->from_table = std::move(inner_from_subquery);

					cast_select_node->select_list.push_back(std::move(sum_function));
					auto cast_select_stmt = make_unique<SelectStatement>();
					cast_select_stmt->node = std::move(cast_select_node);
					cast_subquery_expr->subquery = std::move(cast_select_stmt);
					cast_subquery_expr->subquery_type = SubqueryType::SCALAR;

					auto src_rowid_colref =
					    make_unique<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
					auto dst_rowid_colref =
					    make_unique<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
					auto edge_rowid_colref = make_unique<ColumnRefExpression>("rowid", edge_element->variable_binding);

					auto cast_expression =
					    make_unique<CastExpression>(LogicalType::BIGINT, std::move(cast_subquery_expr));

					vector<unique_ptr<ParsedExpression>> csr_edge_children;
					csr_edge_children.push_back(std::move(csr_edge_id_constant));
					csr_edge_children.push_back(std::move(count_create_edge_select));
					csr_edge_children.push_back(std::move(cast_expression));
					csr_edge_children.push_back(std::move(src_rowid_colref));
					csr_edge_children.push_back(std::move(dst_rowid_colref));
					csr_edge_children.push_back(std::move(edge_rowid_colref));

					auto outer_select_node = make_unique<SelectNode>();

					auto create_csr_edge_function =
					    make_unique<FunctionExpression>("create_csr_edge", std::move(csr_edge_children));
					create_csr_edge_function->alias = "temp";

					outer_select_node->select_list.push_back(std::move(create_csr_edge_function));
					outer_select_node->from_table =
					    GetJoinRef(edge_table, edge_element->variable_binding,
					               previous_vertex_element->variable_binding, next_vertex_element->variable_binding);

					outer_select_statement->node = std::move(outer_select_node);
					auto info = make_unique<CommonTableExpressionInfo>();
					info->query = std::move(outer_select_statement);

					auto cte_select_node = make_unique<SelectNode>();
					cte_select_node->cte_map.map["cte1"] = std::move(info);
					cte_select_node->modifiers.push_back(make_unique<DistinctModifier>());
					for (auto &col : ref.column_list) {
						cte_select_node->select_list.push_back(std::move(col));
					}

					auto cte_ref = make_unique<BaseTableRef>();
					cte_ref->table_name = "cte1";
					auto cross_ref = make_unique<JoinRef>(JoinRefType::CROSS);
					cross_ref->left = std::move(cte_ref);
					auto src_vertex_ref = make_unique<BaseTableRef>();
					src_vertex_ref->table_name = edge_table->source_reference;
					src_vertex_ref->alias = previous_vertex_element->variable_binding;

					auto dst_vertex_ref = make_unique<BaseTableRef>();
					dst_vertex_ref->table_name = edge_table->destination_reference;
					dst_vertex_ref->alias = next_vertex_element->variable_binding;

					auto cross_join_src_dst = make_unique<JoinRef>(JoinRefType::CROSS);
					cross_join_src_dst->left = std::move(src_vertex_ref);
					cross_join_src_dst->right = std::move(dst_vertex_ref);

					cross_ref->right = std::move(cross_join_src_dst);
					cte_select_node->from_table = std::move(cross_ref);

					auto src_row_id =
					    make_unique<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
					auto cte_src_row = make_unique<ColumnRefExpression>("src_row", "cte1");

					auto dst_row_id = make_unique<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
					auto cte_dst_row = make_unique<ColumnRefExpression>("dst_row", "cte1");

					vector<unique_ptr<ParsedExpression>> reachability_children;
					auto cte_where_src_row =
					    make_unique<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
					auto cte_where_dst_row =
					    make_unique<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
					auto reachability_subquery_expr = make_unique<SubqueryExpression>();
					reachability_subquery_expr->subquery =
					    GetCountTable(edge_table, previous_vertex_element->variable_binding);
					reachability_subquery_expr->subquery_type = SubqueryType::SCALAR;

					auto reachability_id_constant = make_unique<ConstantExpression>(Value::INTEGER((int32_t)0));

					reachability_children.push_back(std::move(reachability_id_constant));
					reachability_children.push_back(std::move(reachability_subquery_expr));
					reachability_children.push_back(std::move(cte_where_src_row));
					reachability_children.push_back(std::move(cte_where_dst_row));

					auto reachability_function =
					    make_unique<FunctionExpression>("iterativelength", std::move(reachability_children));
					auto cte_col_ref = make_unique<ColumnRefExpression>("temp", "cte1");

					auto zero_constant = make_unique<ConstantExpression>(Value::INTEGER((int32_t)0));

					vector<unique_ptr<ParsedExpression>> multiply_children;

					multiply_children.push_back(std::move(zero_constant));
					multiply_children.push_back(std::move(cte_col_ref));
					auto multiply_function = make_unique<FunctionExpression>("multiply", std::move(multiply_children));

					vector<unique_ptr<ParsedExpression>> addition_children;
					addition_children.push_back(std::move(multiply_function));
					addition_children.push_back(std::move(reachability_function));

					auto addition_function = make_unique<FunctionExpression>("add", std::move(addition_children));
					auto lower_limit = make_unique<ConstantExpression>(Value::INTEGER(subpath->lower));
					auto upper_limit = make_unique<ConstantExpression>(Value::INTEGER(subpath->upper));
					auto between_expression = make_unique<BetweenExpression>(
					    std::move(addition_function), std::move(lower_limit), std::move(upper_limit));
					conditions.push_back(std::move(between_expression));

					unique_ptr<ParsedExpression> cte_and_expression;
					for (auto &condition : conditions) {
						if (cte_and_expression) {
							cte_and_expression = make_unique<ConjunctionExpression>(
							    ExpressionType::CONJUNCTION_AND, std::move(cte_and_expression), std::move(condition));
						} else {
							cte_and_expression = std::move(condition);
						}
					}
					//                    cte_select_node->select_list = std::move(ref.column_list);
					cte_select_node->where_clause = std::move(cte_and_expression);
					cte_select_statement->node = std::move(cte_select_node);

					// WITH cte1 AS (
					// SELECT CREATE_CSR_EDGE(0, (SELECT count(c.cid) as vcount FROM Customer c),
					// CAST ((SELECT sum(CREATE_CSR_VERTEX(0, (SELECT count(c.cid) as vcount FROM Customer c),
					// sub.dense_id , sub.cnt )) as numEdges
					// FROM (
					//     SELECT c.rowid as dense_id, count(t.from_id) as cnt
					//     FROM Customer c
					//     LEFT JOIN  Transfers t ON t.from_id = c.cid
					//     GROUP BY c.rowid
					//) sub) AS BIGINT),
					// src.rowid, dst.rowid ) as temp, src.rowid as src_row, dst.rowid as dst_row
					// FROM
					//   Transfers t
					//   JOIN Customer src ON t.from_id = src.cid
					//   JOIN Customer dst ON t.to_id = dst.cid
					//)
					// SELECT src.cid AS c1id, dst.cid AS c2id
					// FROM cte1, Transfers t
					//   JOIN Customer src ON t.from_id = src.cid
					//   JOIN Customer dst ON t.to_id = dst.cid
					// WHERE src.rowid = cte1.src_row AND dst.rowid = cte1.dst_row  AND
					//( reachability(0, (SELECT count(c.cid) FROM Customer c), cte1.src_row, cte1.dst_row) = cte1.temp);
				}
			}

			// check aliases
			alias_map[next_vertex_element->variable_binding] = next_vertex_table->table_name;
			alias_map[edge_element->variable_binding] = edge_table->table_name;

			switch (edge_element->match_type) {
			case PGQMatchType::MATCH_EDGE_ANY: {
				auto src_left_expr =
				    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
				                              next_vertex_element->variable_binding, edge_element->variable_binding);
				auto dst_left_expr = CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
				                                               previous_vertex_element->variable_binding,
				                                               edge_element->variable_binding);

				auto combined_left_expr = make_unique<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(src_left_expr), std::move(dst_left_expr));

				auto src_right_expr = CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
				                                                previous_vertex_element->variable_binding,
				                                                edge_element->variable_binding);
				auto dst_right_expr =
				    CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
				                              next_vertex_element->variable_binding, edge_element->variable_binding);
				auto combined_right_expr = make_unique<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(src_right_expr), std::move(dst_right_expr));

				auto combined_expr = make_unique<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_OR, std::move(combined_left_expr), std::move(combined_right_expr));

                break;
			}
			case PGQMatchType::MATCH_EDGE_LEFT:
				CheckEdgeTableConstraints(next_vertex_table->table_name, previous_vertex_table->table_name, edge_table);
				conditions.push_back(CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
				                                               next_vertex_element->variable_binding,
				                                               edge_element->variable_binding));
				conditions.push_back(CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
				                                               previous_vertex_element->variable_binding,
				                                               edge_element->variable_binding));
				break;
			case PGQMatchType::MATCH_EDGE_RIGHT:
				CheckEdgeTableConstraints(previous_vertex_table->table_name, next_vertex_table->table_name, edge_table);
				conditions.push_back(CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
				                                               previous_vertex_element->variable_binding,
				                                               edge_element->variable_binding));
				conditions.push_back(CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
				                                               next_vertex_element->variable_binding,
				                                               edge_element->variable_binding));
				break;
			case PGQMatchType::MATCH_EDGE_LEFT_RIGHT: {
				auto src_left_expr =
				    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
				                              next_vertex_element->variable_binding, edge_element->variable_binding);
				auto dst_left_expr = CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
				                                               previous_vertex_element->variable_binding,
				                                               edge_element->variable_binding);

				auto combined_left_expr = make_unique<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(src_left_expr), std::move(dst_left_expr));

				auto additional_edge_alias = edge_element->variable_binding + std::to_string(extra_alias_counter);
				extra_alias_counter++;

				alias_map[additional_edge_alias] = edge_table->table_name;

				auto src_right_expr =
				    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
				                              previous_vertex_element->variable_binding, additional_edge_alias);
				auto dst_right_expr =
				    CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
				                              next_vertex_element->variable_binding, additional_edge_alias);
				auto combined_right_expr = make_unique<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(src_right_expr), std::move(dst_right_expr));

				auto combined_expr = make_unique<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(combined_left_expr), std::move(combined_right_expr));
				conditions.push_back(std::move(combined_expr));
				break;
			}

			default:
				throw InternalException("Unknown match type found");
			}
			previous_vertex_element = next_vertex_element;

			// Check the edge type
			// If (a)-[b]->(c) 	-> 	b.src = a.id AND b.dst = c.id
			// If (a)<-[b]-(c) 	-> 	b.dst = a.id AND b.src = c.id
			// If (a)-[b]-(c)  	-> 	(b.src = a.id AND b.dst = c.id) OR
			// 						(b.dst = a.id AND b.src = c.id)
			// If (a)<-[b]->(c)	->  (b.src = a.id AND b.dst = c.id) AND
			//						(b.dst = a.id AND b.src = c.id)
		}
	}

	if (path_finding) {
		auto result = make_unique<SubqueryRef>(std::move(cte_select_statement), ref.alias);
		return Bind(*result);
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
			where_clause = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(where_clause),
			                                                  std::move(condition));
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
