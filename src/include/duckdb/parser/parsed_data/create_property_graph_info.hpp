//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_property_graph_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/expression/property_graph_table_expression.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {

class SchemaCatalogEntry;

struct CreatePropertyGraphInfo : public ParseInfo {
	explicit CreatePropertyGraphInfo();
//	DUCKDB_API CreatePropertyGraphInfo(string catalog, string schema, string name);
	explicit CreatePropertyGraphInfo(string property_graph_name);

//	explicit CreatePropertyGraphInfo(CatalogType type, string schema = DEFAULT_SCHEMA, string catalog_p = INVALID_CATALOG)
//	    : type(type), catalog(std::move(catalog_p)), schema(schema), on_conflict(OnCreateConflict::ERROR_ON_CONFLICT),
//	      temporary(false), internal(false) {
//	}
	~CreatePropertyGraphInfo() override {
	}

	//! Property graph name
	string property_graph_name;
	//! List of vector tables
	vector<PropertyGraphTable> vector_tables;
	//! List of edge tables
	vector<PropertyGraphTable> edge_tables;

	//! Dictionary to point label to vector or edge table
	unordered_map<string, PropertyGraphTable*> label_map;

protected:
	void SerializeInternal(Serializer &serializer) const;

public:
	DUCKDB_API static unique_ptr<CreatePropertyGraphInfo> Deserialize(Deserializer &deserializer);

	DUCKDB_API unique_ptr<CreatePropertyGraphInfo> Copy() const;
};
} // namespace duckdb