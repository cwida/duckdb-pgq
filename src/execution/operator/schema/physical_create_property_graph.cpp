#include "duckdb/execution/operator/schema/physical_create_property_graph.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

class CreatePropertyGraphSourceState : public GlobalSourceState {
public:
	CreatePropertyGraphSourceState() : finished(false) {
	}

	bool finished;
};

PhysicalCreatePropertyGraph::PhysicalCreatePropertyGraph(unique_ptr<CreatePropertyGraphInfo> info,
                                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_PROPERTY_GRAPH, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info)) {
}

unique_ptr<GlobalSourceState> PhysicalCreatePropertyGraph::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreatePropertyGraphSourceState>();
}

void PhysicalCreatePropertyGraph::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                          LocalSourceState &lstate) const {
	auto &state = (CreatePropertyGraphSourceState &)gstate;
	if (state.finished) {
		return;
	}

	auto &client_data = ClientData::Get(context.client);

	//! During the binder we already check if the property graph exists
	client_data.registered_property_graphs[info->property_graph_name] = info.get();

	state.finished = true;
}

} // namespace duckdb
