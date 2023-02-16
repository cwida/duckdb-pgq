#pragma once

#include "duckdb/execution/physical_operator.hpp"


namespace duckdb {


class PhysicalCreatePropertyGraph : public PhysicalOperator {
public:
	PhysicalCreatePropertyGraph();
};
}