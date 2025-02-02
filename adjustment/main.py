from fastapi import FastAPI

from .dag import FunctionDAG, InvalidGraph
from .request import AdjustmentRequest
from .response import AdjustmentResponse
from .sequence import FunctionSequence, InvalidSequence
from .utils import logger_factory

logger = logger_factory(__name__)

app = FastAPI()


def construct_graph(
    adjustment_request: AdjustmentRequest,
) -> FunctionSequence | FunctionDAG | InvalidGraph | InvalidSequence:
    if isinstance(adjustment_request.operations, str):
        return FunctionSequence.from_string(
            sequence_string=adjustment_request.operations
        )
    else:
        return FunctionDAG.from_node_list(
            dag_op_list=adjustment_request.operations,
            argument_mappings=adjustment_request.argument_mappings,
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

    if isinstance(dag, InvalidGraph) or isinstance(dag, InvalidSequence):
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
