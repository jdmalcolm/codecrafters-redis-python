import socket  # noqa: F401
import threading


def redis_encode(data, encoding="utf-8"):
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


def connect(connection: socket.socket) -> None:
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

            if arr[1].lower() == b"ping":
                response = redis_encode("PONG")
                print(f"Sending back: {response}")
                connection.send(response)
            elif arr[1].lower() == b"echo":
                response = redis_encode([el.decode("utf-8")
                                        for el in arr[3::2]])
                print(f"Sending back: {response}")
                connection.send(response)
            else:
                break


def main() -> None:
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

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
