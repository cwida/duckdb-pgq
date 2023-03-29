#include "duckdb/parser/statement/drop_property_graph_statement.hpp"
#include "duckdb/planner/binder.hpp"


namespace duckdb {

BoundStatement Binder::Bind(DropPropertyGraphStatement &stmt) {
    BoundStatement result;
    auto &base = (DropInfo &)*stmt.info;
    if (stmt.info->type != CatalogType::PROPERTY_GRAPH_ENTRY) {
        throw BinderException("Incorrect CatalogType");
    }
    auto sqlpgq_state_entry = context.registered_state.find("sqlpgq");
    if (sqlpgq_state_entry == context.registered_state.end()) {
        throw MissingExtensionException("The SQL/PGQ extension has not been loaded");
    }
    auto sqlpgq_state = reinterpret_cast<SQLPGQContext *>(sqlpgq_state_entry->second.get());



}
}
