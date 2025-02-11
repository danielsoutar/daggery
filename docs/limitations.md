# Limitations and Challenges

The following list the current limitations and challenges the library faces:

## Daggery requires nodes output a single value

This is *unlikely* to be a huge concern short-term, since:

* If data is completely unrelated, your node is likely doing too much (single-reponsibility principle).
* If data is closely related, it arguably should be in a common object.
* You can always wrap the data in a tuple to satisfy the constraint.

However, this ties in with the next and bigger issue:

## Daggery broadcasts outputs, not assign them

The main reason for this is simplicity of implementation, but also because things can become more ambiguous. Consider what you would expect to happen if a node outputs three values, and has three children. Should it:

1. Assign or map each output to a specific child? Is this fixed, or user-defined?
2. Broadcast each output to each child? When should this happen?

Although (a) sounds sensible and in line with the inputs, this complicates the implementation, particularly if broadcasting semantics are still desirable, albeit restricted (perhaps broadcasting only occurs if the number of children > 1 and there is a single output).

## Daggery has proper concurrent evaluation, but this is not optimal and is ordering-sensitive

The batching policy inside the `AsyncFunctionDAG` class provides solid performance and is provably correct, but this is dependent on the nodes being suitably ordered. Daggery only promises the batching is sound, not performant. The user currently needs to order the nodes in the graph description in a way where independent nodes are grouped contiguously (i.e. next to each other).

An optimal policy would be to dynamically execute a task graph, and this is being investigated.

## Daggery does not evaluate nodes in parallel

Common to Python libraries, Daggery does not have a mechanism for 'true' parallelism, aside from the effective parallelism you achieve in running multiple non-blocking tasks concurrently. This is harder to change, but could be addressed by a lower-level implementation, say in a V2 version of this library in Rust..?

## Daggery does not guard against incorrect usage of mutable arguments

An illustration of this is provided in one of the examples. As stated in the example, if the computation is ordering-dependent:
### ***it needs to be sequential! This applies to synchronous DAGs too!***
When you have branching in a DAG, you are implicitly declaring the computation to be ordering-*independent*. With that being said, it should generally be safe to provide mutable values provided they are read-only, or written to in distinct regions. This however should be avoided, and Daggery does not have any mechanism for defending against this.

## Daggery is not benchmarked

It is unlikely to have an issue with performance in the transform - the transform is extremely short and incurs little in the way of branching, complex indices, nested structures, or waits (apart from the necessary `await` in gathering each batch for the async DAG).

A more credible concern is memory performance - see [here](https://github.com/pydantic/pydantic/issues/11194) for some details. In particular the question of scaling to large numbers of models is unknown. For practical usage this is likely not a concern, but could be greater if multiple levels of nesting occur, as would be the case with composition.

## Daggery does not perform type validation on the nodes themselves

This is probably the most credible issue Daggery faces, being a validation library for DAGs, but is potentially hard to implement, especially in a user-friendly way. This is especially true in the face of functions that may miss type signatures, and are not guaranteed to obey these signatures anyway. Unlike graph compilers for deep neural networks, we have many different types and combinations thereof to deal with. And Python is rather famous for being freestyle.
