from socket import *
import socket
import threading
import logging

def proses_string(request_string):
    balas = "ERROR\r\n"
    if (request_string.startswith("TIME") and request_string.endswith("\r\n")):
        from datetime import datetime
        now = datetime.now()
        waktu = now.strftime("%H:%M:%S")
        balas=f"JAM {waktu}\r\n"
    if (request_string.startswith("QUIT") and request_string.endswith("\r\n")):
        balas="XXX"
    return balas


class ProcessTheClient(threading.Thread):
    def __init__(self,connection,address):
        self.connection = connection
        self.address = address
        threading.Thread.__init__(self)
    def run(self):
        while True:
            data = self.connection.recv(64)
            if data:
                request_s = data.decode()
                balas = proses_string(request_s)
                if (balas == "XXX"):
                    logging.warning(f"Closing Connection")
                    self.connection.close()
                    break
                print(f"Sending: {balas}")
                self.connection.sendall(balas.encode())
            else:
                break
        self.connection.close()

class Server(threading.Thread):
	def __init__(self):
		self.the_clients = []
		self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		threading.Thread.__init__(self)

	def run(self):
		self.my_socket.bind(('0.0.0.0',45000))
		self.my_socket.listen(1)
		while True:
			self.connection, self.client_address = self.my_socket.accept()
			logging.warning(f"connection from {self.client_address}")
			
			clt = ProcessTheClient(self.connection, self.client_address)
			clt.start()
			self.the_clients.append(clt)
	

def main():
	svr = Server()
	svr.start()

if __name__=="__main__":
	main()

