from socket import *
import socket
import time
import sys
import logging
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from http import HttpServer


#untuk menggunakan processpoolexecutor, karena tidak mendukung subclassing pada process,
#maka class ProcessTheClient dirubah dulu menjadi function, tanpda memodifikasi behaviour didalamnya

def ProcessTheClient(listening_socket):
    httpserver = HttpServer()
    print(f"Worker process {os.getpid()} started and is waiting for connections.")
    
    while True:
        try:
            connection, client_address = listening_socket.accept()
            print(f"Worker {os.getpid()} accepted connection from {client_address}")
            
            rcv = ""
            while True:
                data = connection.recv(4096)
                if data:
                    d = data.decode('utf-8', errors="ignore")
                    rcv += d
                    if rcv.endswith('\r\n\r\n\r\n'):
                        hasil = httpserver.proses(rcv)
                        connection.sendall(hasil)
                        connection.close()
                        break
                else:
                    connection.close()
                    break
            print(f"Worker {os.getpid()} finished handling {client_address}")

        except OSError:
            break
        except Exception as e:
            print(f"Error in worker {os.getpid()}: {e}")
            try:
                connection.close()
            except NameError:
                pass

def main():
    listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listening_socket.bind(('0.0.0.0', 8889))
    listening_socket.listen(1)
    print("Main process started. Process Pool Server running on port 8889.")

    num_workers = 20
    worker_processes = []
    for _ in range(num_workers):
        p = multiprocessing.Process(target=ProcessTheClient, args=(listening_socket,))
        worker_processes.append(p)
        p.start()

    try:
        for p in worker_processes:
            p.join()
    except KeyboardInterrupt:
        print("\nShutting down server and all worker processes.")
        for p in worker_processes:
            p.terminate()
        listening_socket.close()


if __name__=="__main__":
    main()

