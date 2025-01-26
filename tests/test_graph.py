from adjustment.graph import (
    EmptyDAG,
    UnvalidatedDAG,
    UnvalidatedNode,
)


def test_unvalidated_dag_from_string_single_node():
    dag_string = "example"
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(name="example0", rule="example", children=[]),
        ]
    )
    assert UnvalidatedDAG.from_string(dag_string) == expected_output


def test_unvalidated_dag_from_string_multiple_nodes():
    dag_string = "root >> first_child >> last_child"
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(name="root0", rule="root", children=["first_child0"]),
            UnvalidatedNode(
                name="first_child0", rule="first_child", children=["last_child0"]
            ),
            UnvalidatedNode(name="last_child0", rule="last_child", children=[]),
        ]
    )
    assert UnvalidatedDAG.from_string(dag_string) == expected_output


def test_unvalidated_dag_from_string_empty_string():
    dag_string = ""
    expected_output = EmptyDAG(message="DAG string is empty and therefore invalid")
    assert UnvalidatedDAG.from_string(dag_string) == expected_output
