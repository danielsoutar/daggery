from fastapi import FastAPI
from pydantic import ConfigDict

from daggery.dag import FunctionDAG
from daggery.description import AdjustmentRequest
from daggery.graph import InvalidGraph
from daggery.node import Node
from daggery.response import AdjustmentResponse
from daggery.utils.decorators import logged, timed
from daggery.utils.logging import logger_factory

logger = logger_factory(__name__)

app = FastAPI()


class Foo(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def transform(self, value: int) -> int:
        return value * value


class Bar(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def transform(self, value: int) -> int:
        return value + 10


class Baz(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def transform(self, value: int) -> int:
        return value - 5


class Qux(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def transform(self, value: int) -> int:
        return value * 2


class Quux(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def transform(self, value: int) -> int:
        return value // 2


custom_node_map: dict[str, type[Node]] = {
    "foo": Foo,
    "bar": Bar,
    "baz": Baz,
    "qux": Qux,
    "quux": Quux,
}


def construct_graph(
    adjustment_request: AdjustmentRequest,
) -> FunctionDAG | InvalidGraph:
    if isinstance(adjustment_request.operations, str):
        return FunctionDAG.from_string(
            graph_description=adjustment_request.operations,
            custom_node_map=custom_node_map,
        )
    else:
        return FunctionDAG.from_node_list(
            graph_description=adjustment_request.operations,
            argument_mappings=adjustment_request.argument_mappings,
            custom_node_map=custom_node_map,
        )


@app.post("/adjustment", response_model=AdjustmentResponse)
async def process_adjustment_request(adjustment_request: AdjustmentRequest):
    """
    Process an AdjustmentRequest.

    This endpoint receives a `AdjustmentRequest`, performs the specified series
    of operations, and returns a confirmation message with the result.

    ### Request Body
    - name: The name of the `AdjustmentRequest` object.
    - value: The value to transform with operations.
    - operations: A string representing the series of operations to perform.

    ### Response
    - message: Confirmation message including the result of the operations.
    """
    dag = construct_graph(adjustment_request)

    if isinstance(dag, InvalidGraph):
        logger.error("Failed to create DAG")
        return AdjustmentResponse(message=f"Failed to create DAG: {dag.message}")

    logger.info("DAG successfully created")
    result = dag.transform(adjustment_request.value)
    return AdjustmentResponse(
        message=(
            f"Received AdjustmentRequest with name: {adjustment_request.name} "
            f"and value: {adjustment_request.value}. "
            f"Result after transformation: {result}"
        )
    )
