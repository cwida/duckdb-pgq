//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/compressed_sparse_row.h"

namespace duckdb {
class AttachedDatabase;
class BufferedFileWriter;
class ClientContext;
class CatalogSearchPath;
class FileOpener;
class HTTPStats;
class QueryProfiler;
class QueryProfilerHistory;
class PreparedStatementData;
class SchemaCatalogEntry;
class CSR;
struct RandomEngine;



struct ClientData {
	ClientData(ClientContext &context);
	~ClientData();

	//! Query profiler
	shared_ptr<QueryProfiler> profiler;
	//! QueryProfiler History
	unique_ptr<QueryProfilerHistory> query_profiler_history;

	//! The set of temporary objects that belong to this client
	shared_ptr<AttachedDatabase> temporary_objects;
	//! The set of bound prepared statements that belong to this client
	unordered_map<string, shared_ptr<PreparedStatementData>> prepared_statements;

	//! The writer used to log queries (if logging is enabled)
	unique_ptr<BufferedFileWriter> log_query_writer;
	//! The random generator used by random(). Its seed value can be set by setseed().
	unique_ptr<RandomEngine> random_engine;

	//! The catalog search path
	unique_ptr<CatalogSearchPath> catalog_search_path;

	//! The file opener of the client context
	unique_ptr<FileOpener> file_opener;

	//! Statistics on HTTP traffic
	unique_ptr<HTTPStats> http_stats;

	//! The file search path
	string file_search_path;

	//! Used to build the CSR data structures required for path-finding queries
	std::unordered_map<int32_t, unique_ptr<CSR>> csr_list;
	std::mutex csr_lock;

public:
	DUCKDB_API static ClientData &Get(ClientContext &context);
};

} // namespace duckdb
