/* we treat csr -> v, csr -> e and csr -> w as tables.
 * The col names and tables names are also treated as tables.
 * And later, we define some table function to get graph-related data.
 *
 * This header defines all the structs and classes used later.
 */

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/compressed_sparse_row.h"
#include "sqlpgq_common.hpp"
#include "sqlpgq_functions.hpp"
#include "duckdb/common/types/value.hpp"


namespace duckdb {

struct CSRScanVData : public TableFunctionData {
public:
	static unique_ptr<FunctionData> ScanCSRVBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
		auto result = make_unique<CSRScanVData>();
                result->csrID = input.inputs[0].GetValue<int32_t>();
                return_types.emplace_back(LogicalType::BIGINT);
                names.emplace_back("csrv");
                return result;
	}
public:
	int32_t csrID;
};

struct CSRScanEData : public TableFunctionData {
public:
        static unique_ptr<FunctionData> ScanCSREBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
                auto result = make_unique<CSRScanEData>();
                result->csrID = input.inputs[0].GetValue<int32_t>();
                return_types.emplace_back(LogicalType::BIGINT);
                names.emplace_back("csre");
                return result;
        }
public:
        int32_t csrID;
};

struct CSRScanWData : public TableFunctionData {
public:
        static unique_ptr<FunctionData> ScanCSRWBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
                auto result = make_unique<CSRScanWData>();
                result->csrID = input.inputs[0].GetValue<int32_t>();

		auto sqlpgq_state_entry = context.registered_state.find("sqlpgq");
        	if (sqlpgq_state_entry == context.registered_state.end()) {
                	//! Wondering how you can get here if the extension wasn't loaded, but leaving this check in anyways
                	throw InternalException("The SQL/PGQ extension has not been loaded");
        	}
        	auto sqlpgq_state = reinterpret_cast<SQLPGQContext *>(sqlpgq_state_entry->second.get());

        	CSR *csr = sqlpgq_state -> GetCSR(result -> csrID);

		if(csr->w.size()) {
			result -> is_double = false;
                	return_types.emplace_back(LogicalType::BIGINT);
		} else {
			result -> is_double = true;
			return_types.emplace_back(LogicalType::DOUBLE);
		}
                names.emplace_back("csrw");
                return result;
        }
public:
        int32_t csrID;
	bool is_double;
};

struct CSRScanWDoubleData : public TableFunctionData {
public:
        static unique_ptr<FunctionData> ScanCSRWDoubleBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
                auto result = make_unique<CSRScanWDoubleData>();
                result->csrID = input.inputs[0].GetValue<int32_t>();
                return_types.emplace_back(LogicalType::DOUBLE);
                names.emplace_back("csrw");
                return result;
        }
public:
        int32_t csrID;
};


struct PGScanVTableData : public TableFunctionData {
public:
        static unique_ptr<FunctionData> ScanPGVTableBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
                auto result = make_unique<PGScanVTableData>();
                result->pg_name = StringValue::Get(input.inputs[0]);
                return_types.emplace_back(LogicalType::VARCHAR);
                names.emplace_back("vtables");
                return result;
        }
public:
        string pg_name;
};

struct PGScanVColData : public TableFunctionData {
public:
        static unique_ptr<FunctionData> ScanPGVColBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
                auto result = make_unique<PGScanVColData>();
                result->pg_name = StringValue::Get(input.inputs[0]);
		result->table_name = StringValue::Get(input.inputs[1]);
                return_types.emplace_back(LogicalType::VARCHAR);
                names.emplace_back("colnames");
                return result;
        }
public:
        string pg_name;
	string table_name;
};

struct PGScanETableData : public TableFunctionData {
public:
        static unique_ptr<FunctionData> ScanPGETableBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
                auto result = make_unique<PGScanVTableData>();
                result->pg_name = StringValue::Get(input.inputs[0]);
                return_types.emplace_back(LogicalType::VARCHAR);
                names.emplace_back("etables");
                return result;
        }
public:
        string pg_name;
};

struct PGScanEColData : public TableFunctionData {
public:
        static unique_ptr<FunctionData> ScanPGEColBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
                auto result = make_unique<PGScanVColData>();
                result->pg_name = StringValue::Get(input.inputs[0]);
                result->table_name = StringValue::Get(input.inputs[1]);
                return_types.emplace_back(LogicalType::VARCHAR);
                names.emplace_back("colnames");
                return result;
        }
public:
        string pg_name;
        string table_name;
};

struct CSRScanState : public GlobalTableFunctionState {
public:
        static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
                auto result = make_unique<CSRScanState>();
                return result;
        }

public:
        bool finished = false;
};


} // namespace duckdb
