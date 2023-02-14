#include "duckdb/parser/expression/property_graph_table_expression.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

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

} // namespace duckdb
