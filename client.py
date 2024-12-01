import requests


url = "http://127.0.0.1:8000/foo"

# Define the Foo object to send
foo_data = {"name": "example", "value": 42, "operations": "foo"}

# Send a POST request to the FastAPI server
response = requests.post(url, json=foo_data)

# Print the response from the server
print("Status Code:", response.status_code)
print("Response JSON:", response.json())
