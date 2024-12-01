from fastapi.testclient import TestClient

from adjustment.main import AdjustmentRequest, AdjustmentResponse, app

# Create a TestClient instance
client = TestClient(app)

# TODO: Consider installing pytest-xdist for parallel test execution


def test_adjustment():
    adj_input = AdjustmentRequest(name="example", value=42, operations="foo")
    response = client.post("/adjustment", json=adj_input.model_dump())
    assert response.status_code == 200
    adj_output = AdjustmentResponse(**response.json())
    assert (
        adj_output.message
        == "Received AdjustmentRequest with name: example and value: 42. "
        "Result after transformation: 1764"
    )
