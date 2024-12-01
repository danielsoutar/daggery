# adjustment example

This is a simple refactoring of a service I've seen before. It sends a sequence of desired functions to be executed, in a pipeline with the following syntax:

```
"foo_func » bar_func » baz_func » verification_func"
```

The string should only reference valid functions defined in the service, and the service will execute them on the request payload.


## Usage

To run the service locally, run the following command:

```
poetry run uvicorn adjustment.main:app --reload
```

The service will listen on http://127.0.0.1:8000/adjustment - the provided client in client.py can be used to test the service.