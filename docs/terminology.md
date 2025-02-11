# Terminology

In Daggery there are some common terms and concepts used. This page summarises them.

## DAG

A directed, acyclic (cycle-free) graph. In Daggery, a `FunctionDAG` is conceptually a DAG of `Operation`s, implemented via `Node`s encoding functions.

## `Operation`

An `Operation` is a semantic, high-level concept. It represents a desired action given inputs and outputs. It can be thought of as a component in the 'front-end' of a DAG for clients.

## `Node`

By contrast, a `Node` is the backing implementation of an `Operation` via its `transform`. The transform can either be a single function, a function calling multiple other functions, or even another DAG.

In the latter case, it is worth pointing out that an `Operation` can be thought of as mapping directly to a `Node`, or a sub-graph of conceptually lower-level `Operation`s that in turn are implemented via `Node`s.

## `DAGDescription`

A high-level description of a DAG. It does not provide implementation details, but simply describes the desired graphical structure of a DAG. It references valid `Operation`s, which are user- or service-defined.

## Topological sorting

A topological sorting is a sorting over graph nodes such that, provided in a sequence, for any given node, the node's parents must be specified before it in the sequence. By extension, it follows that the node's children must be specified after it in the sequence.

## Batches

A `batch` is defined as a contiguous group of nodes in a topologically sorted list such that none of them have any parent/child relationships between them. This implies they are independent of each other, and may be evaluated in parallel.

A topologically sorted list can always be defined entirely in batches with every node being in a batch of size 1. In Daggery, the `AsyncFunctionDAG` ensures every node belongs to a (non-overlapping) batch in the DAG's collection of nodes, and batches are evaluated in sequential order.
