#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/parser/property_graph_table.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

PropertyGraphTable::PropertyGraphTable() {

}

PropertyGraphTable::PropertyGraphTable(string table_name_p, vector<string> column_names_p, vector<string> labels_p)
    : table_name(std::move(table_name_p)), column_names(std::move(column_names_p)), labels(std::move(labels_p)) {
#ifdef DEBUG
	for (auto &col_name : column_names) {
		D_ASSERT(!col_name.empty());
	}

	for (auto &label : labels) {
		D_ASSERT(!label.empty());
	}
#endif
}

void PropertyGraphTable::Serialize(Serializer &serializer) const {
	serializer.WriteString(table_name);

	serializer.WriteStringVector(column_names);
	serializer.WriteStringVector(labels);

	serializer.Write<bool>(is_vertex_table);
	if (!is_vertex_table) {
		serializer.WriteStringVector(source_pk);
		serializer.WriteStringVector(source_fk);
		serializer.WriteString(source_reference);

		serializer.WriteStringVector(destination_pk);
		serializer.WriteStringVector(destination_fk);
		serializer.WriteString(destination_reference);
	}
}

unique_ptr<PropertyGraphTable> PropertyGraphTable::Copy() {
	auto result = make_unique<PropertyGraphTable>();
	result->table_name = table_name;
	for (auto &column_name : column_names) {
		result->column_names.push_back(column_name);
	}
	for (auto &label : labels) {
		result->labels.push_back(label);
	}
	result->is_vertex_table = is_vertex_table;
	result->discriminator = discriminator;

	result->source_reference = source_reference;

	for (auto &key : source_fk) {
		result->source_fk.push_back(key);
	}

	for (auto &key : source_pk) {
		result->source_pk.push_back(key);
	}

	result->destination_reference = destination_reference;

	for (auto &key : destination_fk) {
		result->destination_fk.push_back(key);
	}

	for (auto &key : destination_pk) {
		result->destination_pk.push_back(key);
	}
	return result;
}

} // namespace duckdb