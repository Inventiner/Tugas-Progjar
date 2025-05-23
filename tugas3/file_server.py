from socket import *
import socket
import threading
import logging
import time
import sys


from file_protocol import  FileProtocol
fp = FileProtocol()


class ProcessTheClient(threading.Thread):
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        threading.Thread.__init__(self)

    def run(self):
        buffer = ""
        try:
            while True:
                data = self.connection.recv(4096)
                if not data:
                    break
                try:
                    buffer += data.decode()
                except Exception as e:
                    logging.error(e)
                    break
                
                if "\r\n\r\n" in buffer:
                    command_to_process = buffer.split("\r\n\r\n", 1)[0]
                    hasil = fp.proses_string(command_to_process)
                    buffer = ""
                    response_to_send=hasil+"\r\n\r\n"
                    self.connection.sendall(response_to_send.encode())
        except Exception as e:
            logging.error(f"Unexpected error processing client {self.address}: {e}", exc_info=True)
        finally:
            self.connection.close()
            logging.info(f"Connection with {self.address} ended.")


class Server(threading.Thread):
    def __init__(self,ipaddress='0.0.0.0',port=8889):
        self.ipinfo=(ipaddress,port)
        self.the_clients = []
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        threading.Thread.__init__(self)

    def run(self):
        logging.warning(f"server berjalan di ip address {self.ipinfo}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(1)
        try:
            while True:
                self.connection, self.client_address = self.my_socket.accept()
                logging.warning(f"connection from {self.client_address}")
    
                clt = ProcessTheClient(self.connection, self.client_address)
                clt.start()
                self.the_clients.append(clt)
        except Exception as e:
            logging.critical(f"Server main loop encountered an error: {e}", exc_info=True)
        finally:
            self.my_socket.close()
            logging.warning("Server shutdown.")

def main():
    svr = Server(ipaddress='0.0.0.0',port=6666)
    svr.start()


if __name__ == "__main__":
    main()

