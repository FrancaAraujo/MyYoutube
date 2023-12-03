import socket
import threading
import os
import random

HOST = "0.0.0.0" #localhost
PORT = 9999

datanode_list = []

datanode_file = open('database.txt', 'r')
for line in datanode_file.readlines():
    ip, port = line.strip().split(' ')
    datanode_list.append((ip,int(port)))

print("datanode list = ",datanode_list)

# Função para buscar máquinas e portas com base no nome do vídeo
def buscar_ip_e_porta_por_video(nome_video, arquivo):
    maquinas_e_portas = []
    with open(arquivo, 'r') as arquivo_txt:
        linhas = arquivo_txt.readlines()
        for linha in linhas:
            partes = linha.strip().split()
            print(partes[0])
            print(f"{nome_video}.mp4")
            stringaux1 = list(nome_video)
            stringaux2 = stringaux1[0]
            print(stringaux2)
            if len(partes) == 3 and partes[0] == stringaux2:
                maquinas_e_portas.append((partes[1], int(partes[2])))
    return maquinas_e_portas

def handle_client(client_socket):
    request = client_socket.recv(1024).decode('utf-8')

    if request.startswith("UPLOAD "):
        # Handle file upload request
        file_name = request[7:]
        print(f"Receiving file: {file_name}")

        #file = open(f"uploads/{file_name}", "wb")
        done = False

        rep_factor = 3
        temp = b""
        while not done:
            data = client_socket.recv(1024)
            if data[-5:] == b"<END>":
                done = True
                temp += data[:-5]
                #file.write(data[:-5])
            else:
                temp += data
                #file.write(data)

        datanodes = random.sample(datanode_list, rep_factor)
        datanode_s_list = []
        for i, datanode in enumerate(datanodes):
            datanode_s_list.append(socket.socket())
            datanode_s_list[i].connect(datanode)
        
        for datanode_s in datanode_s_list:
            datanode_s.send(f"UPLOAD {file_name}".encode())
            datanode_s.sendall(temp)
            datanode_s.send(b"<END>")                
            datanode_s.close()
        # Escreva os valores de NomeDoVideo, IP e Porta separados por espaço
        with open("index.txt", "a") as arquivo_indice:
            for item in datanodes:
                print(item)
                ip, port = item
                arquivo_indice.write(f"{file_name} {ip} {port}\n")
        #file.close()
        print(f"File '{file_name}' received and saved.")

    elif request.startswith("STREAM "):
        # Handle streaming request
        video_file = request[7:]
        print(f"Streaming video: {video_file}")

        maquinas_e_portas = buscar_ip_e_porta_por_video({video_file}, "index.txt")
        ip, port = random.choice(maquinas_e_portas)
        datanode_list.append((ip,int(port)))

        datanodes2 = random.sample(datanode_list, 1)
        datanode = datanodes2[0]
        
        datanode_s = socket.socket()
        datanode_s.connect(datanode)

        datanode_s.send(f"STREAM {video_file}".encode())

        done = False
        temp2 = b""
        while not done:
            data = datanode_s.recv(4096)
            if data[-5:] == b"<END>":
                done = True
                client_socket.send(data[:-5])
                #file.write(data[:-5])
            else:
                client_socket.send(data)
                #file.write(data)

        client_socket.close()
        print(f"Video '{video_file}' streamed.")

    elif request.startswith("LISTAR "):
        with open("index.txt", "r") as arquivo:
            linhas = arquivo.readlines()

        nomes_arquivos = set()
        for linha in linhas:
            partes = linha.strip().split()
            if len(partes) >= 1:
                nomes_arquivos.add(partes[0])

        nomes_arquivos_str = ",".join(nomes_arquivos)

        # Envie o tamanho do arquivo antes de enviar o nome
        client_socket.send(nomes_arquivos_str.encode('utf-8'))

        client_socket.close()

    elif request.startswith("SEARCH "):
        file_name = request[7:]
        with open("index.txt", "r") as arquivo:
            linhas = arquivo.readlines()

        nomes_arquivos = set()
        for linha in linhas:
            partes = linha.strip().split()
            if len(partes) >= 1:
                nomes_arquivos.add(partes[0])
        # Filtrar os nomes de arquivo que contêm `file_name`
        resultados_pesquisa = [nome for nome in nomes_arquivos if file_name in nome]

        # Envie os nomes dos arquivos para o cliente Flask
        nomes_arquivos_str = ','.join(resultados_pesquisa)
        client_socket.send(nomes_arquivos_str.encode('utf-8'))

        client_socket.close()


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen()

print("Server listening for incoming connections...")

while True:
    client, addr = server.accept()
    client_handler = threading.Thread(target=handle_client, args=(client,))
    client_handler.start()