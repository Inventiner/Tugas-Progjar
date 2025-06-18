import socket
import logging
import time

def kirim_data(message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logging.warning("membuka socket")

    server_address = ('localhost', 45000)
    logging.warning(f"opening socket {server_address}")
    sock.connect(server_address)

    try:
        logging.warning(f"sending message: {message}")
        sock.sendall(message.encode())
        data_received=""

        data = sock.recv(16)
        if data:
            data_received += data.decode()
        logging.warning(f"data received from server: {data_received}")
    except:
        logging.warning("error during data receiving")

if __name__=='__main__':
    for i in range(1,5):
        kirim_data("TIME\r\n")
        time.sleep(4)

    kirim_data("QUIT\r\n")