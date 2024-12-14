import argparse
import requests

BASE_URL = "http://localhost"

def log_request(response, *args, **kwargs):
    """
    Log details of the final response after the redirect chain.
    """
    req = response.request
    print(f"{req.method}\t{req.url}\t{response.status_code}")
    if req.body:
        print(f"Request Body: {req.body}")

    if response.content:
        print(f"Response: {response.content}\n")

def make_request(method, url, **kwargs):
    """
    Make a request and set up hooks to log the final response.
    """
    kwargs.setdefault('hooks', {'response': log_request})
    response = requests.request(method, url, **kwargs)
    return response

def main():
    parser = argparse.ArgumentParser(description="Client for Go HTTP Server")
    parser.add_argument('port', type=int, help="HTTP server port")
    parser.add_argument('operation', type=str, help="Operation to perform: create, read, readbyid, update, delete, cas")
    parser.add_argument('params', nargs='+', help="Parameters for the operation")
    args = parser.parse_args()

    operation = args.operation.lower()
    port = args.port
    params = args.params

    if operation == "create" and len(params) == 2:
        key, value = params
        url = f"{BASE_URL}:{port}/create"
        data = {'key': key, 'value': value}
        response = make_request('POST', url, json=data)
    elif operation == "read" and len(params) == 1:
        key = params[0]
        url = f"{BASE_URL}:{port}/read/{key}"
        response = make_request('GET', url)
    elif operation == "readbyid" and len(params) == 2:
        key, log_id = params
        url = f"{BASE_URL}:{port}/read/{key}/{log_id}"
        response = make_request('GET', url)
    elif operation == "update" and len(params) == 2:
        key, value = params
        url = f"{BASE_URL}:{port}/update"
        data = {'key': key, 'value': value}
        response = make_request('PUT', url, json=data)
    elif operation == "delete" and len(params) == 1:
        key = params[0]
        url = f"{BASE_URL}:{port}/delete/{key}"
        response = make_request('DELETE', url)
    elif operation == "cas" and len(params) == 3:
        key, new_value, expected_value = params
        url = f"{BASE_URL}:{port}/cas"
        data = {'key': key, 'value': new_value, 'expectedValue': expected_value}
        response = make_request('PUT', url, json=data)
    else:
        print("Invalid operation or parameters")
        return

if __name__ == "__main__":
    main()
