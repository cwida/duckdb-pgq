#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/parser/property_graph_table.hpp"

namespace duckdb {

PropertyGraphTable::PropertyGraphTable() = default;

PropertyGraphTable::PropertyGraphTable(string table_name_p, vector<string> column_names_p, vector<string> labels_p)
    : table_name(std::move(table_name_p)), column_names(std::move(column_names_p)), sub_labels(std::move(labels_p)) {

#ifdef DEBUG
	for (auto &col_name : column_names) {
		D_ASSERT(!col_name.empty());
	}

	for (auto &label : sub_labels) {
		D_ASSERT(!label.empty());
	}
#endif
}

PropertyGraphTable::PropertyGraphTable(string table_name_p, string table_name_alias_p, vector<string> column_names_p,
                                       vector<string> labels_p)
    : table_name(std::move(table_name_p)), table_name_alias(std::move(table_name_alias_p)),
      column_names(std::move(column_names_p)), sub_labels(std::move(labels_p)) {
#ifdef DEBUG
	for (auto &col_name : column_names) {
		D_ASSERT(!col_name.empty());
	}
	for (auto &except_column : except_columns) {
		D_ASSERT(!except_column.empty());
	}

	for (auto &label : sub_labels) {
		D_ASSERT(!label.empty());
	}
#endif
}

string PropertyGraphTable::ToString() const {
	string result = table_name + " " + (table_name_alias.empty() ? "" : "AS " + table_name_alias);
	if (!is_vertex_table) {
		result += "SOURCE KEY (";
		for (idx_t i = 0; i < source_fk.size(); i++) {
			if (i != source_fk.size() - 1) {
				result += source_fk[i] + ", ";
			} else {
				// Last element should be without a trailing , instead )
				result = source_fk[i] + ") ";
			}
		}
		result += "REFERENCES " + source_reference + " (";
		for (idx_t i = 0; i < source_pk.size(); i++) {
			if (i != source_pk.size() - 1) {
				result += source_pk[i] + ", ";
			} else {
				result = source_pk[i] + ") ";
			}
		}
		result += "\n";
		result += "DESTINATION KEY (";
		for (idx_t i = 0; i < destination_fk.size(); i++) {
			if (i != destination_fk.size() - 1) {
				result += destination_fk[i] + ", ";
			} else {
				// Last element should be without a trailing , instead )
				result = destination_fk[i] + ") ";
			}
		}
		result += "REFERENCES " + destination_reference + " (";
		for (idx_t i = 0; i < destination_pk.size(); i++) {
			if (i != destination_pk.size() - 1) {
				result += destination_pk[i] + ", ";
			} else {
				result = destination_pk[i] + ") ";
			}
		}
	}
	result += "\n";
	result += "PROPERTIES (";
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (i != column_names.size() - 1) {
			result += column_names[i] + (column_aliases[i].empty() ? "" : "AS " + column_aliases[i]) + ", ";
		} else {
			result = column_names[i] + (column_aliases[i].empty() ? "" : "AS " + column_aliases[i]) + ") ";
		}
	}

	result += "LABEL " + main_label;
	if (!sub_labels.empty()) {
		result += " IN " + discriminator + "( ";
		for (idx_t i = 0; i < sub_labels.size(); i++) {
			if (i != sub_labels.size() - 1) {
				result += sub_labels[i] + ", ";
			} else {
				result = sub_labels[i] + ") ";
			}
		}
	}

	return result;
}

bool PropertyGraphTable::Equals(const PropertyGraphTable *other_p) const {

	auto other = (PropertyGraphTable *)other_p;
	if (table_name != other->table_name) {
		return false;
	}

	if (table_name_alias != other->table_name_alias) {
		return false;
	}

	if (column_names.size() != other->column_names.size()) {
		return false;
	}
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (column_names[i] != other->column_names[i]) {
			return false;
		}
	}
	if (column_aliases.size() != other->column_aliases.size()) {
		return false;
	}
	for (idx_t i = 0; i < column_aliases.size(); i++) {
		if (column_aliases[i] != other->column_aliases[i]) {
			return false;
		}
	}
	if (except_columns.size() != other->except_columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < except_columns.size(); i++) {
		if (except_columns[i] != other->except_columns[i]) {
			return false;
		}
	}
	if (sub_labels.size() != other->sub_labels.size()) {
		return false;
	}
	for (idx_t i = 0; i < sub_labels.size(); i++) {
		if (sub_labels[i] != other->sub_labels[i]) {
			return false;
		}
	}

	if (main_label != other->main_label) {
		return false;
	}
	if (all_columns != other->all_columns) {
		return false;
	}
	if (no_columns != other->no_columns) {
		return false;
	}
	if (is_vertex_table != other->is_vertex_table) {
		return false;
	}
	if (discriminator != other->discriminator) {
		return false;
	}
	if (source_fk.size() != other->source_fk.size()) {
		return false;
	}
	for (idx_t i = 0; i < source_fk.size(); i++) {
		if (source_fk[i] != other->source_fk[i]) {
			return false;
		}
	}
	if (source_pk.size() != other->source_pk.size()) {
		return false;
	}
	for (idx_t i = 0; i < source_pk.size(); i++) {
		if (source_pk[i] != other->source_pk[i]) {
			return false;
		}
	}
	if (source_reference != other->source_reference) {
		return false;
	}

	if (destination_fk.size() != other->destination_fk.size()) {
		return false;
	}
	for (idx_t i = 0; i < destination_fk.size(); i++) {
		if (destination_fk[i] != other->destination_fk[i]) {
			return false;
		}
	}

	if (destination_pk.size() != other->destination_pk.size()) {
		return false;
	}
	for (idx_t i = 0; i < destination_pk.size(); i++) {
		if (destination_pk[i] != other->destination_pk[i]) {
			return false;
		}
	}
	if (destination_reference != other->destination_reference) {
		return false;
	}

	return true;
}

void PropertyGraphTable::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "table_name", table_name);
	serializer.WriteProperty(101, "column_names", column_names);
	serializer.WriteProperty(102, "column_aliases", column_aliases);
	serializer.WriteProperty(103, "except_columns", except_columns);
	serializer.WriteProperty(104, "sub_labels", sub_labels);

//	serializer.WriteList(101, "column_names", column_names.size(),
//											 [&](Serializer::List &list, idx_t i) { list.WriteElement(column_names[i]); });
//	serializer.WriteList(102, "column_aliases", column_aliases.size(),
//											 [&](Serializer::List &list, idx_t i) { list.WriteElement(column_aliases[i]); });
//	serializer.WriteList(103, "except_columns", except_columns.size(),
//											 [&](Serializer::List &list, idx_t i) { list.WriteElement(except_columns[i]); });
//	serializer.WriteList(104, "sub_labels", sub_labels.size(),
//											 [&](Serializer::List &list, idx_t i) { list.WriteElement(sub_labels[i]); });
	serializer.WriteProperty(105, "main_label", main_label);
	serializer.WriteProperty(106, "is_vertex_table", is_vertex_table);
	serializer.WriteProperty(107, "all_columns", all_columns);
	serializer.WriteProperty(108, "no_columns", no_columns);

	if (!is_vertex_table) {
		serializer.WriteProperty(109, "source_pk", source_pk);
		serializer.WriteProperty(110, "source_fk", source_fk);
		serializer.WriteProperty(111, "source_reference", source_reference);

		serializer.WriteProperty(112, "destination_pk", destination_pk);
		serializer.WriteProperty(113, "destination_fk", destination_fk);
		serializer.WriteProperty(114, "destination_reference", destination_reference);
	}
}


shared_ptr<PropertyGraphTable> PropertyGraphTable::Deserialize(Deserializer &deserializer) {
	auto pg_table = make_shared<PropertyGraphTable>();
	deserializer.ReadProperty(100, "table_name", pg_table->table_name);
	deserializer.ReadProperty(101, "column_names", pg_table->column_names);
	deserializer.ReadProperty(102, "column_aliases", pg_table->column_aliases);
	deserializer.ReadProperty(103, "except_columns", pg_table->except_columns);
	deserializer.ReadProperty(104, "sub_labels", pg_table->sub_labels);
	// read the column names
//	deserializer.ReadList(101, "column_names", [&](Deserializer::List &list, idx_t i) {
//			auto column_name = list.ReadElement<string>();
//			pg_table->column_names.push_back(column_name);
//	});
//
//	vector<string> column_aliases;
//	deserializer.ReadList(102, "column_aliases", [&](Deserializer::List &list, idx_t i) {
//			auto column_alias = list.ReadElement<string>();
//			pg_table->column_aliases.push_back(column_alias);
//	});
//
//	vector<string> except_columns;
//	deserializer.ReadList(103, "column_aliases", [&](Deserializer::List &list, idx_t i) {
//			auto except_column = list.ReadElement<string>();
//			pg_table->except_columns.push_back(except_column);
//	});
//
//	vector<string> sub_labels;
//	deserializer.ReadList(104, "column_aliases", [&](Deserializer::List &list, idx_t i) {
//			auto sub_label = list.ReadElement<string>();
//			pg_table->sub_labels.push_back(sub_label);
//	});

	deserializer.ReadProperty(105, "main_label", pg_table->main_label);
	deserializer.ReadProperty(106, "is_vertex_table", pg_table->is_vertex_table);
	deserializer.ReadProperty(107, "no_columns", pg_table->no_columns);

	if (!pg_table->is_vertex_table) {
		deserializer.ReadProperty(108, "source_pk", pg_table->source_pk);
		deserializer.ReadProperty(109, "source_fk", pg_table->source_fk);
		deserializer.ReadProperty(110, "source_reference", pg_table->source_reference);

		deserializer.ReadProperty(108, "destination_pk", pg_table->destination_pk);
		deserializer.ReadProperty(109, "destination_fk", pg_table->destination_fk);
		deserializer.ReadProperty(110, "destination_reference", pg_table->destination_reference);
	}
	return pg_table;
}

shared_ptr<PropertyGraphTable> PropertyGraphTable::Copy() const {
	auto result = make_shared<PropertyGraphTable>();

	result->table_name = table_name;
	for (auto &column_name : column_names) {
		result->column_names.push_back(column_name);
	}
	for (auto &except_column : except_columns) {
		result->except_columns.push_back(except_column);
	}
	for (auto &label : sub_labels) {
		result->sub_labels.push_back(label);
	}

	result->main_label = main_label;
	result->is_vertex_table = is_vertex_table;
	result->all_columns = all_columns;
	result->no_columns = no_columns;
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
	return std::move(result);
}

} // namespace duckdb



//void PropertyGraphTable::Serialize(Serializer &serializer) const {
//	FieldWriter writer(serializer);
//	writer.WriteString(table_name);
//
//	writer.WriteList<string>(column_names);
//	writer.WriteList<string>(column_aliases);
//	writer.WriteList<string>(except_columns);
//	writer.WriteList<string>(sub_labels);
//	writer.WriteString(main_label);
//
//	writer.WriteField<bool>(is_vertex_table);
//	writer.WriteField<bool>(all_columns);
//	writer.WriteField<bool>(no_columns);
//	if (!is_vertex_table) {
//		writer.WriteList<string>(source_pk);
//		writer.WriteList<string>(source_fk);
//		writer.WriteString(source_reference);
//
//		writer.WriteList<string>(destination_pk);
//		writer.WriteList<string>(destination_fk);
//		writer.WriteString(destination_reference);
//	}
//	writer.Finalize();
//}