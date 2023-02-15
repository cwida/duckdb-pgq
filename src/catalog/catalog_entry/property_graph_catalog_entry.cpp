#include "duckdb/catalog/catalog_entry/property_graph_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"

namespace duckdb {


PropertyGraphCatalogEntry::PropertyGraphCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                     CreatePropertyGraphInfo *info)
    : StandardEntry(CatalogType::PROPERTY_GRAPH_ENTRY, schema, catalog, info->property_graph_name) {
	Initialize(info);
}

void PropertyGraphCatalogEntry::Initialize(CreatePropertyGraphInfo *info) {
	this->name = info->property_graph_name;
	this->vertex_tables = std::move(info->vertex_tables);
	this->edge_tables = std::move(info->edge_tables);
}

unique_ptr<CatalogEntry> PropertyGraphCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(!internal);

	auto create_info = make_unique<CreatePropertyGraphInfo>();
	create_info->property_graph_name = name;

	return make_unique<PropertyGraphCatalogEntry>(catalog, schema, create_info.get());
}

void PropertyGraphCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(!internal);
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	D_ASSERT(vertex_tables.size() <= NumericLimits<uint32_t>::Maximum());
	serializer.Write<uint32_t>((uint32_t)vertex_tables.size());
	for (auto &vertex_table : vertex_tables) {
		vertex_table->Serialize(serializer);
	}
	serializer.Write<uint32_t>((uint32_t)edge_tables.size());
	for (auto &edge_table : edge_tables) {
		edge_table->Serialize(serializer);
	}
}

unique_ptr<CreatePropertyGraphInfo> PropertyGraphCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreatePropertyGraphInfo>();
	info->schema = source.Read<string>();
	auto vertex_table_count = source.Read<uint32_t>();
	for (uint32_t i = 0; i < vertex_table_count; i++) {
		info->vertex_tables.push_back(PropertyGraphTable::Deserialize(source));
	}
	auto edge_table_count = source.Read<uint32_t>();
	for (uint32_t i = 0; i < edge_table_count; i++) {
		info->edge_tables.push_back(PropertyGraphTable::Deserialize(source));
	}
	return info;
}

string PropertyGraphCatalogEntry::ToSQL() {
	// TODO
	//  - Implement this function for PropertyGraphCatalogEntry
	return "";
}

}