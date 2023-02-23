#pragma once


namespace duckdb {

class MatchRef : public TableRef {
public:
	MatchRef() : TableRef(TableReferenceType::MATCH) {

	}


};


}