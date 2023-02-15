#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/property_graph_table.hpp"


namespace duckdb {

struct CreatePropertyGraphInfo;


class PropertyGraphCatalogEntry : public StandardEntry {
public:
	PropertyGraphCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePropertyGraphInfo *info);

	static constexpr const CatalogType Type = CatalogType::PROPERTY_GRAPH_ENTRY;
	static constexpr const char *Name = "property_graph";

	vector<unique_ptr<PropertyGraphTable>> vertex_tables;
	vector<unique_ptr<PropertyGraphTable>> edge_tables;

	vector<LogicalType> types;


public:
	//! Serialize the meta information of the PropertyGraphCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreatePropertyGraphInfo
	static unique_ptr<CreatePropertyGraphInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;

	string ToSQL() override;

private:
	void Initialize(CreatePropertyGraphInfo *info);

};


}