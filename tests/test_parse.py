from adjustment.parse import UnvalidatedDAG, UnvalidatedNode, parse_linear_list_string


def test_parse_single_node():
    dag_string = "example"
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(name="example0", rule="example", children=[]),
        ]
    )
    assert parse_linear_list_string(dag_string) == expected_output


def test_parse_multiple_nodes():
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
    assert parse_linear_list_string(dag_string) == expected_output
