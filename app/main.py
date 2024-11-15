import socket  # noqa: F401
import threading


def connect(connection: socket.socket) -> None:
    with connection:
        connected: bool = True
        while connected:
            request: bytes = connection.recv(512)
            print(f"Received: {request.decode()}")

            connected = bool(request)
            if "ping" in request.decode().lower():
                pong: str = "+PONG\r\n"
                connection.send(pong.encode())



def main() -> None:
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_socket, client_addr = server_socket.accept() # wait for client
    print(f"Accepted connection [{client_addr[0]}:{str(client_addr[1])}]")

    connect(client_socket)

if __name__ == "__main__":
    main()
