import argparse
import socket  # noqa: F401
import threading
import time

DB = {}
DEFAULT_PORT = 6379


def redis_encode(data, encoding="utf-8"):
    """
    Applies redis serialization to a string and encodes it.
    """
    if not data:
        return "$-1\r\n".encode(encoding=encoding)
    if not isinstance(data, list):
        data = [data]
    separator = "\r\n"
    size = len(data)
    encoded = []
    for datum in data:
        encoded.append(f"${len(datum)}")
        encoded.append(datum)
    if size > 1:
        encoded.insert(0, f"*{size}")
    print(f"encoded: {encoded}")
    return (separator.join(encoded) + separator).encode(encoding=encoding)


def delete_key(key: str):
    """
    Deletes entry from database.
    """
    if DB[key]:
        del DB[key]
        print(f"Entry '{key.decode()}' removed from database.")


def request_set(request):
    """
    Processes a SET request, adding {key: value} pair to database.
    Basic request format: 'SET key value'
    Add PX option to apply expiry in ms: 'SET key value PX 100'
    """
    request_len = len(request)
    if request_len < 7:
        print("ERROR: SET request with invalid num args")
        return redis_encode("ERROR")
    key = request[3]
    value = request[5]
    DB[key] = value
    print(f"Added to database: '{key}':'{value}")

    # Apply expiration if PX option used
    if (request_len >= 9) and (request[7].lower() == b"px"):
        expiry_ms = float(request[9])
        print(f"Entry '{key}' set to expire in {expiry_ms} ms")
        t = threading.Timer(expiry_ms / 1000, delete_key, args=(key,))
        t.start()

    return redis_encode("OK")


def request_get(request):
    """
    Processes a GET request, returning value corresponding to the given key in the database.
    Returns null response if key not found.
    """
    if len(request) != 5:
        print("ERROR: GET request with invalid num args")
        return redis_encode("ERROR")
    key = request[3]
    value = DB.get(key)
    if not value:
        print(f"GET - Key '{key.decode()}' not found")
        return redis_encode("")
    print(f"Retrieved '{key.decode()}' from database: '{value.decode()}'")
    return redis_encode(value.decode())


def connect(connection: socket.socket) -> None:
    """
    Processes connection and provides responses to various requests.
    """
    with connection:
        connected: bool = True
        while connected:
            request: bytes = connection.recv(512)
            print("Received {!r}".format(request))

            connected = bool(request)

            arr_size, *arr = request.split(b"\r\n")
            if arr_size == b"":
                break
            print(f"Arr size: {arr_size}")
            print(f"Arr content: {arr}")

            # PING
            if arr[1].lower() == b"ping":
                response = redis_encode("PONG")

            # ECHO
            elif arr[1].lower() == b"echo":
                response = redis_encode([el.decode("utf-8")
                                        for el in arr[3::2]])

            # SET
            elif arr[1].lower() == b"set":
                response = request_set(arr)

            # GET
            elif arr[1].lower() == b"get":
                response = request_get(arr)
            
            # INFO
            elif arr[1].lower() == b"info":
                response = redis_encode("role:master")

            else:
                break

            print(f"Sending back: {response}")
            connection.send(response)


def arg_parser() -> argparse.Namespace:
    """
    Parse arguments from bash cli.
    """
    parser = argparse.ArgumentParser(
        prog="Build your own Redis",
        description="Launches Redis-like application. After running, \
            interact with the application using the `redis-cli` command in a \
            new terminal."
    )
    parser.add_argument('--port')
    args = parser.parse_args()
    return args


def main() -> None:
    print("Logs from your program will appear here!")

    port = DEFAULT_PORT
    args = arg_parser()
    if args.port:
        port = int(args.port)

    server_socket = socket.create_server(("localhost", port), reuse_port=True)

    while True:
        client_socket, client_addr = server_socket.accept()  # wait for client
        print(f"Accepted connection [{client_addr[0]}:{str(client_addr[1])}]")

        # connect(client_socket)
        thread: threading.Thread = threading.Thread(
            target=connect, args=[client_socket]
        )
        thread.start()


if __name__ == "__main__":
    main()
