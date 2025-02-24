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
    def evaluate(self, value: int) -> int:
        return value * value


class Bar(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def evaluate(self, value: int) -> int:
        return value + 10


class Baz(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def evaluate(self, value: int) -> int:
        return value - 5


class Qux(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def evaluate(self, value: int) -> int:
        return value * 2


class Quux(Node, frozen=True):
    @timed(logger)
    @logged(logger)
    def evaluate(self, value: int) -> int:
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
class EvaluateRequest(BaseModel):
    name: str
    value: int
    operations: str | DAGDescription


class EvaluateResponse(BaseModel):
    message: str


def construct_graph(
    evaluate_request: EvaluateRequest,
) -> FunctionDAG | InvalidDAG:
    if isinstance(evaluate_request.operations, str):
        return FunctionDAG.from_string(
            dag_description=evaluate_request.operations,
            custom_op_node_map=custom_op_node_map,
        )
    else:
        return FunctionDAG.from_dag_description(
            dag_description=evaluate_request.operations,
            custom_op_node_map=custom_op_node_map,
        )


@app.post("/evaluate", response_model=EvaluateResponse)
async def process_evaluate_request(evaluate_request: EvaluateRequest):
    """
    This endpoint receives an `EvaluateRequest`, performs the specified series
    of operations, and returns a confirmation message with the result.

    ### Request Body
    - name: The name of the `DAGDescription` object.
    - value: The value to call the operations with.
    - operations: A string or `DAGDescription` representing the series of operations to perform.

    ### Response
    - message: Confirmation message including the result of the operations.
    """
    dag = construct_graph(evaluate_request)

    if isinstance(dag, InvalidDAG):
        logger.error("Failed to create DAG")
        return EvaluateResponse(message=f"Failed to create DAG: {dag.message}")

    logger.info("DAG successfully created")
    result = dag.evaluate(evaluate_request.value)
    return EvaluateResponse(
        message=(
            f"Received DAGDescription with name: {evaluate_request.name} "
            f"and value: {evaluate_request.value}. "
            f"Result after evaluation: {result}"
        )
    )
