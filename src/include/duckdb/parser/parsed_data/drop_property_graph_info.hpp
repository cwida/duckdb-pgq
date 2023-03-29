#pragma once


#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct DropPropertyGraphInfo : public ParseInfo {
    DropPropertyGraphInfo();

    //! Element name to drop
    string name;

public:
    unique_ptr<DropPropertyGraphInfo> Copy() const {
        auto result = make_unique<DropPropertyGraphInfo>();
        result->name = name;

        return result;
    }

    void Serialize(Serializer &serializer) const {
        FieldWriter writer(serializer);
        writer.WriteString(name);
        writer.Finalize();
    }

    static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer) {
        FieldReader reader(deserializer);
        auto drop_pg_info = make_unique<DropInfo>();
        drop_pg_info->name = reader.ReadRequired<string>();
        return drop_pg_info;
    }
};
}