#include "duckdb/parser/statement/drop_property_graph_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
    unique_ptr<SQLStatement> Transformer::TransformDropPropertyGraph(duckdb_libpgquery::PGNode *node) {
        auto stmt = (duckdb_libpgquery::PGDropPropertyGraphStmt *)(node);
        auto result = make_unique<DropPropertyGraphStatement>();



    }


}