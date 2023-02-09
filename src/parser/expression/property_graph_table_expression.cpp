#include "duckdb/parser/expression/property_graph_table_expression.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

PropertyGraphTableExpression::PropertyGraphTableExpression(string column_name, string label, string table_name)
    : PropertyGraphTableExpression(table_name.empty() ? vector<string> {std::move(column_name)}, vector<string> {std::move(label)}
                                             : vector<string> {std::move(table_name), std::move(column_name)}, vector<string> {std::move(label)}) {
}

PropertyGraphTableExpression::PropertyGraphTableExpression(string column_name, string label)
    : PropertyGraphTableExpression(vector<string> {std::move(column_name)}, vector<string> {std::move(label)}) {
}

PropertyGraphTableExpression::PropertyGraphTableExpression(vector<string> column_names_p, vector<string> labels_p)
    : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF),
      column_names(std::move(column_names_p)), labels(std::move(labels_p)) {
#ifdef DEBUG
	for (auto &col_name : column_names) {
		D_ASSERT(!col_name.empty());
	}

	for (auto &label : labels) {
		D_ASSERT(!label.empty());
	}
#endif
}

bool PropertyGraphTableExpression::IsQualified() const {

	return column_names.size() > 1;
}

const string &PropertyGraphTableExpression::GetColumnName() const {
	D_ASSERT(column_names.size() <= 4);
	return column_names.back();
}

const string &PropertyGraphTableExpression::GetTableName() const {
	D_ASSERT(column_names.size() >= 2 && column_names.size() <= 4);
	if (column_names.size() == 4) {
		return column_names[2];
	}
	if (column_names.size() == 3) {
		return column_names[1];
	}
	return column_names[0];
}

string PropertyGraphTableExpression::GetName() const {
	return !alias.empty() ? alias : column_names.back();
}

string PropertyGraphTableExpression::ToString() const {
	string result;
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (i > 0) {
			result += ".";
		}
		result += KeywordHelper::WriteOptionallyQuoted(column_names[i]);
	}
	return result;
}

bool PropertyGraphTableExpression::Equal(const PropertyGraphTableExpression *a, const PropertyGraphTableExpression *b) {
	if (a->column_names.size() != b->column_names.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->column_names.size(); i++) {
		auto lcase_a = StringUtil::Lower(a->column_names[i]);
		auto lcase_b = StringUtil::Lower(b->column_names[i]);
		if (lcase_a != lcase_b) {
			return false;
		}
	}
	return true;
}

hash_t PropertyGraphTableExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	for (auto &column_name : column_names) {
		auto lcase = StringUtil::Lower(column_name);
		result = CombineHash(result, duckdb::Hash<const char *>(lcase.c_str()));
	}
	return result;
}

unique_ptr<ParsedExpression> PropertyGraphTableExpression::Copy() const {
	auto copy = make_unique<PropertyGraphTableExpression>(column_names, labels);
	copy->CopyProperties(*this);
	return std::move(copy);
}

void PropertyGraphTableExpression::Serialize(FieldWriter &writer) const {
	writer.WriteList<string>(column_names);
}

} // namespace duckdb
