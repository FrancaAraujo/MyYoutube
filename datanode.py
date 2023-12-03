import socket
import threading
import os

HOST = "0.0.0.0" #localhost
PORT = 8081

diretorio = f"uploads{PORT}"  # Substitua pelo nome do diretório que deseja criar

# Verifica se o diretório não existe
if not os.path.exists(diretorio):
    os.makedirs(diretorio)  # Cria o diretório

def handle_client(client_socket):
    request = client_socket.recv(1024).decode('utf-8')

    if request.startswith("UPLOAD "):
        # Handle file upload request
        file_name = request[7:]
        print(f"Receiving file: {file_name}")
        file = open(f"uploads{PORT}/{file_name}", "wb")
        done = False

        while not done:
            data = client_socket.recv(1024)
            if data[-5:] == b"<END>":
                done = True
                file.write(data[:-5])
            else:
                file.write(data)

        file.close()
        client_socket.close()
        print(f"File '{file_name}' received and saved.")

    elif request.startswith("STREAM "):
        # Handle streaming request
        video_file = request[7:]
        print(f"Streaming video: {video_file}")
    
        with open(f"uploads{PORT}/{video_file}", "rb") as video_file2:
            while True:
                chunk = video_file2.read(4096)
                print(".")
                if not chunk:
                    break
                client_socket.send(chunk)
            client_socket.send(b"<END>")
            client_socket.close()
            print(f"Video '{video_file}' streamed.")

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen()

print("Database listening for incoming connections...")

while True:
    client, addr = server.accept()
    client_handler = threading.Thread(target=handle_client, args=(client,))
    client_handler.start()