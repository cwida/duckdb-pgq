#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreatePropertyGraphInfo::CreatePropertyGraphInfo() : CreateInfo(CatalogType::PROPERTY_GRAPH_ENTRY) {
}

//CreatePropertyGraphInfo::CreatePropertyGraphInfo(string property_graph_name) : property_graph_name(std::move(property_graph_name)) {
//}
//
CreatePropertyGraphInfo::CreatePropertyGraphInfo(string catalog_p, string schema_p, string name_p)
    : CreateInfo(CatalogType::PROPERTY_GRAPH_ENTRY, std::move(schema_p), std::move(catalog_p)), property_graph_name(std::move(name_p)) {
}
//
//CreatePropertyGraphInfo::CreatePropertyGraphInfo(SchemaCatalogEntry *schema, string name_p)
//    : CreatePropertyGraphInfo(schema->catalog->GetName(), schema->name, std::move(name_p)) {
//}
//
void CreatePropertyGraphInfo::SerializeInternal(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(property_graph_name);
	for (auto &graph_table : graph_tables) {
		graph_table->Serialize(serializer);
	}
	writer.Finalize();
}
//
//unique_ptr<CreatePropertyGraphInfo> CreatePropertyGraphInfo::Deserialize(Deserializer &deserializer) {
//	auto result = make_unique<CreatePropertyGraphInfo>();
//	result->DeserializeBase(deserializer);
//
//	FieldReader reader(deserializer);
//	result->table = reader.ReadRequired<string>();
//	result->columns = ColumnList::Deserialize(reader);
//	result->constraints = reader.ReadRequiredSerializableList<Constraint>();
//	result->query = reader.ReadOptional<SelectStatement>(nullptr);
//	reader.Finalize();
//
//	return result;
//}
//
unique_ptr<CreateInfo> CreatePropertyGraphInfo::Copy() const {
	auto result = make_unique<CreatePropertyGraphInfo>(catalog, schema, property_graph_name);
	CopyProperties(*result);

	for (auto &graph_table : graph_tables) {
		result->graph_tables.push_back(graph_table->Copy());
	}

	return std::move(result);
}

} // namespace duckdb
