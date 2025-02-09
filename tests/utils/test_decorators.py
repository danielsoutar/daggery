from typing import Callable
from unittest.mock import MagicMock, patch

from pydantic import BaseModel, ConfigDict

from daggery.dag import FunctionDAG
from daggery.node import Node
from daggery.utils.decorators import bypass, http_client, logged, timed
from daggery.utils.logging import logger_factory


class MyCustomErrorType(BaseModel):
    error_message: str


class ServiceNode(Node):
    model_config = ConfigDict(frozen=True)

    @http_client("http://example.com")
    @bypass(MyCustomErrorType, logger_factory("service"))
    def transform(self, value: int, client: Callable) -> int:
        res = client("/test", {"key": value})
        return res


custom_node_map: dict[str, type[Node]] = {
    "service": ServiceNode,
}


def test_logged():
    with patch("daggery.utils.logging.logger_factory") as mock_logger_factory:
        mock_logger = mock_logger_factory.return_value
        mock_info = MagicMock()
        mock_logger.info = mock_info

        class LoggedNode(Node):
            model_config = ConfigDict(frozen=True)

            @logged(mock_logger)
            def transform(self, value: int) -> int:
                return value * 2

        dag = FunctionDAG.from_string(
            "logging",
            custom_node_map={"logging": LoggedNode},
        )
        assert isinstance(dag, FunctionDAG)
        actual_output = dag.transform(5)

        expected_output = 10
        assert expected_output == actual_output
        mock_info.assert_any_call("logging0:")
        mock_info.assert_any_call("  args: (5,)")
        mock_info.assert_any_call("  Output: 10")


def test_timed():
    with (
        patch("daggery.utils.logging.logger_factory") as mock_logger_factory,
        patch("time.time", side_effect=[1.0, 2.0]),
    ):
        mock_logger = mock_logger_factory.return_value
        mock_info = MagicMock()
        mock_logger.info = mock_info

        class TimedNode(Node):
            model_config = ConfigDict(frozen=True)

            @timed(mock_logger)
            def transform(self, value: int) -> int:
                return value + 3

        dag = FunctionDAG.from_string(
            "timing",
            custom_node_map={"timing": TimedNode},
        )
        assert isinstance(dag, FunctionDAG)
        actual_output = dag.transform(5)

        expected_output = 8
        assert expected_output == actual_output
        mock_info.assert_any_call("timing0 duration: 1.0s")


def test_bypass():
    dag = FunctionDAG.from_string(
        "service",
        custom_node_map=custom_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    # Despite the service node never being able to handle an error,
    # we succesfully bypass the node and propagate the error through
    # the graph.
    result = dag.transform(MyCustomErrorType(error_message="some error occurred"))

    assert result.error_message == "some error occurred"


def test_http_client():
    with patch("requests.post") as mock_post:
        mock_response = mock_post.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "success"}

        value = 12
        base_url = "http://example.com"
        ep = "/test"
        pl = {"key": value}

        dag = FunctionDAG.from_string(
            "service",
            custom_node_map=custom_node_map,
        )
        assert isinstance(dag, FunctionDAG)

        result = dag.transform(value)

        mock_post.assert_called_once_with(base_url + ep, json=pl)
        assert result.status_code == 200
        assert result.json() == {"result": "success"}
