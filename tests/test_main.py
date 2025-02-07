from fastapi.testclient import TestClient

from daggery.main import AdjustmentRequest, AdjustmentResponse, app

# Create a TestClient instance
client = TestClient(app)

# TODO: Consider installing pytest-xdist for parallel test execution


def test_adjustment_single_operation():
    adj_input = AdjustmentRequest(
        name="example", value=42, operations="foo", argument_mappings=[]
    )
    response = client.post("/adjustment", json=adj_input.model_dump())
    assert response.status_code == 200
    adj_output = AdjustmentResponse(**response.json())
    assert (
        adj_output.message
        == "Received AdjustmentRequest with name: example and value: 42. "
        "Result after transformation: 1764"
    )


def test_adjustment_multiple_operations():
    adj_input = AdjustmentRequest(
        name="multi-node",
        value=10,
        operations="foo >> bar >> baz",
        argument_mappings=[],
    )
    response = client.post("/adjustment", json=adj_input.model_dump())
    assert response.status_code == 200
    adj_output = AdjustmentResponse(**response.json())
    assert (
        adj_output.message
        == "Received AdjustmentRequest with name: multi-node and value: 10. "
        "Result after transformation: 105"  # Assuming the transformation logic is foo -> bar -> baz
    )


def test_invalid_input():
    adj_input = AdjustmentRequest(
        name="invalid",
        value=10,
        operations="foo >> invalid >> baz",
        argument_mappings=[],
    )
    response = client.post("/adjustment", json=adj_input.model_dump())
    assert response.status_code == 200
    adj_output = AdjustmentResponse(**response.json())
    assert "Failed to create DAG" in adj_output.message
