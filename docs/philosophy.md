# Philosophy

This library adheres to the following mantras:

### Latest and greatest developer tools used (correctly) wherever possible

`uv`, `mypy`, and `ruff` are all examples. Warnings are fixed immediately.

### Everything is a value, including errors - code should be exception-free

Daggery code aims to never raise Exceptions, and provides utilities for user-defined `Node`s to avoid doing so.

### Immutability is first-class.

This encourages many things like local reasoning, safety, efficiency, and testability. Additionally it also encourages state to be decoupled and encoded explicitly, further aiding these aims.

### Leverage structure and validated types - the earlier this is done, the greater the benefits.

Structure (such as sortedness and uniqueness) gives leverage and constraints provide freedom to optimise for subsequent code. Immutability is also structure and is treated accordingly.

### Interfaces should be simple and composable. Avoid hacky gimmicks and unmaintainable approaches like multiple inheritance.

Simple code is unlikely to go wrong. Composable abstractions are scalable.
