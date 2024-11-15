import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_socket = server_socket.accept() # wait for client
    client_socket.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
