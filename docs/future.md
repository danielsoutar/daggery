# Future

The library has several potential avenues for features:

* Enabling output assignment (with defaults for simpler cases and enabling broadcasting semantics where desired)
* Enabling type validation
* Constructing DAGs from declarative configuration files (simplifying construction for clients)
* A parallel DAG via `multiprocessing`

## Enabling output assignment

As mentioned in [Limitations](limitations.md), Daggery cannot currently assign outputs to Nodes. Instead, it currently broadcasts each output to each child node.

This is for a couple of reasons:

1. Broadcasting is the most generic and general approach.
2. It requires no/minimal specification from the client.
3. Supporting assignment along with broadcasting is ambiguous in certain cases (e.g. if a node has 3 outputs and 3 children, do we want to assign each output to each child, or broadcast the outputs?)

However, enabling more granular control would likely make Daggery more flexible and clients can be more expressive with the Nodes and types they use.

In the `ArgumentMapping` construct, the default could be to have 'automatic assignment'. That is to say, if a node has >1 output, the first output will be assigned to the first child encountered in the topological sorting, the second output to the second child encountered, and so on.

If manual assignment is desired, it could be specified, similar to how input assignment is done. This would encourage consistency for clients. If broadcasting is desired, it could be declared with a flag.

This would however complicate the propagation of data in the `evaluate` method of the DAG. This is why having it be explicitly declared in the `DAGDescription` model would be useful, since it would help enable Daggery to set up machinery during construction to make the propagation as efficient as possible at evaluation time.

## Enabling type validation

Type validation for Daggery would be very beneficial for strengthening validation. However, a major challenge to this (aside from implementing the validation correctly) is the use of decorators.

Since decorators can inject data, it is not always going to be the case that the inputs for a Node have a 1-1 correspondance with its parent nodes. One way of addressing this might be to stipulate that all node-related arguments must be positional, whereas decorator-arguments are keyword-only and occur strictly later in the argument sequence.

However, this does not ignore how difficult type validation itself would be. Python does not require that a type signature for a function is present. Nor does it require that the signature be correct. Tools like `mypy` are third-party tools that cannot be assumed in the general case. It is possible that Daggery can make type validation optional, and if performed to be optimistic. For example, if type validation is required but a function is missing a signature, it may be that Daggery assumes `Any` for its inputs and outputs, and so degenerately returns true. Then Daggery would only raise issues if there was a direct contradiction in the type signatures.

In terms of implementation, it is probable that `inspect` would be used. During construction the typing for nodes would be extracted, and type validation can be performed in a DAG-agnostic way (i.e. supported for all types of DAGs exposed by Daggery).

## Enabling DAGs from declarative config files

Although potentially the least useful feature, enabling a construct for declarative configuration of a DAG would be useful for clients. In particular it may help with:

1. Testing - clients are better decoupled from how Daggery's internal machinery works, whereas a configuration file might be supported across versions.
2. Deployment - clients can just pass around a new config file to Daggery rather than having to update any code.
3. Comments - a YAML file supports comments, which means a graph can become more self-documenting to a wider audience.
4. Defaulting - having a configuration file means only the minimal set of constructs need specifying, whereas writing code might be more explicit (and less concise).
5. Referencing - a YAML file supports references, which is helpful for a referential data structure like a graph.
6. Serialisation - JSON is widely supported as an exchange format.

In Daggery there could be a factory method that takes in a configuration file. The configuration file may be in JSON/YAML, and may specify similar to the `DAGDescription` model.

## Enabling parallel DAGs via `multiprocessing`

While `AsyncFunctionDAG` is great for concurrency-heavy workloads, it doesn't offer true parallelism. An obvious fix for this would be to enable parallelism via processes. Although more resource-heavy than threads, in Python this is currently the only officially supported means of compute-based parallelism.

This however would be a bit more involved than using `asyncio`. [Disappointingly in Python a process cannot be reused](https://stackoverflow.com/questions/23650576/python-multiprocessing-process-how-to-reuse-a-process). The best way to mitigate against this would be to use a pool. Consequently the approach that stems to mind would be:

1. Explicitly assign nodes to 'blocks' in the DAG description. Blocks would map to processes. All nodes in a block must comprise a 'proper' sub-graph with one head and one tail. If a node is not marked for a block, it will be evaluated on the main process.
2. Require that nodes in the topological sorting are ordered such that all block-assigned nodes are contiguous with respect to other nodes in the same block.
3. Create a process pool in the `evaluate` method for the DAG.
4. Run the graph sequentially for non-block-mapped nodes. The head is always run on the main process.
5. Once a block-mapped node is reached, use a process for the block from the pool (and wait if no processes are available) and continue asynchronously, skipping to the next node not matching the block ID.
6. When a node requires a result from a block-mapped node, wait on the result from that block.
7. When the tail node is reached (which is always run on the main process), join the process pool and return the result.

This would be highly beneficial for parallel workloads. In particular it is possible that data parallelism and task parallelism can be leveraged, as this is dependent on how nodes are implemented and pass data around rather than the DAG itself.

However, for an efficient data-parallel implementation it would be ideal if data could be assigned, as mentioned above.

How this would look exactly isn't clear. One way of passing sub-graphs along to the processes might be to pass a factory method to the process to construct the sub-graph as a Daggery DAG and call its evaluate method with the inputs from input nodes. Since validation has already been performed by this point it could conceivably be argued that the graph could skip some checks, if this happened to be a bottleneck. Another way would be to simply create the sub-graphs as Daggery `FunctionDAG`s during construction (though whether nested parallelism is supported is unclear). Then the `nodes` property on the DAG could have just the head and tail nodes for the sub-graph.
