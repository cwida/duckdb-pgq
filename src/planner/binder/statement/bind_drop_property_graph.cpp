#include "duckdb/parser/statement/drop_property_graph_statement.hpp"
#include "duckdb/planner/binder.hpp"


namespace duckdb {

BoundStatement Binder::Bind(DropPropertyGraphStatement &stmt) {
    BoundStatement result;
    auto &base = (DropInfo &)*stmt.info;
    if (stmt.info->type != CatalogType::PROPERTY_GRAPH_ENTRY) {
        throw BinderException("Incorrect CatalogType");
    }


}
}
