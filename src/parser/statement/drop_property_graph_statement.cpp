#include "duckdb/parser/statement/drop_property_graph_statement.hpp"

namespace duckdb {

    DropPropertyGraphStatement::DropPropertyGraphStatement() : SQLStatement(StatementType::DROP_STATEMENT), info(make_unique<DropPropertyGraphInfo>()) {
    }

    DropPropertyGraphStatement::DropPropertyGraphStatement(const DropPropertyGraphStatement &other) : SQLStatement(other), info(other.info->Copy()) {
    }

    unique_ptr<SQLStatement> DropPropertyGraphStatement::Copy() const {
        return unique_ptr<DropPropertyGraphStatement>(new DropPropertyGraphStatement(*this));
    }

} // namespace duckdb
