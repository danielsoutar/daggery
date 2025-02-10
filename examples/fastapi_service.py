from fastapi import FastAPI
from pydantic import BaseModel

from daggery.dag import FunctionDAG
from daggery.description import DAGDescription
from daggery.node import Node
from daggery.prevalidate import InvalidDAG
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


custom_op_node_map: dict[str, type[Node]] = {
    "foo": Foo,
    "bar": Bar,
    "baz": Baz,
    "qux": Qux,
    "quux": Quux,
}


# In this example, clients not only provide inputs, but also the desired graph
# to evaluate.
class TransformRequest(BaseModel):
    name: str
    value: int
    operations: str | DAGDescription


class TransformResponse(BaseModel):
    message: str


def construct_graph(
    transform_request: TransformRequest,
) -> FunctionDAG | InvalidDAG:
    if isinstance(transform_request.operations, str):
        return FunctionDAG.from_string(
            dag_description=transform_request.operations,
            custom_op_node_map=custom_op_node_map,
        )
    else:
        return FunctionDAG.from_dag_description(
            dag_description=transform_request.operations,
            custom_op_node_map=custom_op_node_map,
        )


@app.post("/transform", response_model=TransformResponse)
async def process_transform_request(transform_request: TransformRequest):
    """
    This endpoint receives a `TransformRequest`, performs the specified series
    of operations, and returns a confirmation message with the result.

    ### Request Body
    - name: The name of the `DAGDescription` object.
    - value: The value to transform with operations.
    - operations: A string representing the series of operations to perform.

    ### Response
    - message: Confirmation message including the result of the operations.
    """
    dag = construct_graph(transform_request)

    if isinstance(dag, InvalidDAG):
        logger.error("Failed to create DAG")
        return TransformResponse(message=f"Failed to create DAG: {dag.message}")

    logger.info("DAG successfully created")
    result = dag.transform(transform_request.value)
    return TransformResponse(
        message=(
            f"Received DAGDescription with name: {transform_request.name} "
            f"and value: {transform_request.value}. "
            f"Result after transformation: {result}"
        )
    )
