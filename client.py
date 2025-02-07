import requests

url = "http://127.0.0.1:8000/example"

# Define the Request object to send
example_request = {
    "name": "example",
    "value": 42,
    "operations": "foo",
    "argument_mappings": [],
}

# Send a POST request to the FastAPI server
response = requests.post(url, json=example_request)

# Print the response from the server
print("Status Code:", response.status_code)
print("Response JSON:", response.json())
