from fastapi import FastAPI

from adjustment.dag import from_string

from .request import AdjustmentRequest
from .response import AdjustmentResponse

app = FastAPI()


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
    dag = from_string(adjustment_request.operations)
    result = dag.transform(adjustment_request.value)

    return AdjustmentResponse(
        message=(
            f"Received AdjustmentRequest with name: {adjustment_request.name} "
            f"and value: {adjustment_request.value}. "
            f"Result after transformation: {result}"
        )
    )
