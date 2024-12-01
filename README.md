# adjustment example

This is a simple refactoring of a service I've seen before. It sends a sequence of desired functions to be executed, in a pipeline with the following syntax:

```
"foo_func » bar_func » baz_func » verification_func"
```

The string should only reference valid functions defined in the service, and the service will execute them on the request payload.
