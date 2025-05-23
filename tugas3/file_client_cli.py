import socket
import json
import base64
import logging

server_address=('0.0.0.0',7777)

def send_command(command_str=""):
    global server_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(server_address)
        logging.warning(f"connecting to {server_address}")

        command_to_send = command_str + "\r\n\r\n"
        
        logging.warning(f"sending message ")
        sock.sendall(command_to_send.encode())

        data_received=""
        while True:
            data = sock.recv(1024)
            if data:
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    message_part = data_received.split("\r\n\r\n", 1)[0]
                    hasil = json.loads(message_part)
                    logging.warning(f"Data received from server: {hasil}")
                    return hasil
            else:
                break
    except Exception as e:
        logging.warning("error during data receiving")
        return dict(status='ERROR',data=str(e))


def remote_list():
    command_str=f"LIST"
    hasil = send_command(command_str)
    if (hasil['status']=='OK'):
        print("daftar file : ")
        for nmfile in hasil['data']:
            print(f"- {nmfile}")
        return True
    else:
        print("Gagal")
        return False

def remote_get(filename=""):
    command_str=f"GET {filename}"
    hasil = send_command(command_str)
    if (hasil['status']=='OK'):
        #proses file dalam bentuk base64 ke bentuk bytes
        namafile= hasil['data_namafile']
        isifile = base64.b64decode(hasil['data_file'])
        fp = open(namafile,'wb+')
        fp.write(isifile)
        fp.close()
        return True
    else:
        print("Gagal")
        return False

def remote_upload(filename=""):
    if (filename == ""):
        return False
    try:            
        with open(filename, 'rb') as fp:
            file_content_bytes = fp.read()
            
        isifile = base64.b64encode(file_content_bytes).decode()
        command_str=f"UPLOAD {filename} {isifile}"
        hasil = send_command(command_str)

        if (hasil['status']=='OK'):
            return True
        else:
            print(f"Gagal UPLOAD '{filename}': {hasil.get('data', 'Unknown error')}")
            return False  
        
    except Exception as e:
        print(f"Error: {e}")
        return False

def remote_delete(filename=""):
    if not filename:
        return False
    command_str = f"DELETE {filename}"
    hasil = send_command(command_str)
    if hasil and hasil.get('status') == 'OK':
        return True
    else:
        print(f"Gagal DELETE '{filename}': {hasil.get('data', 'Unknown error')}")
        return False

if __name__=='__main__':
    server_address=('172.16.16.101',6666)
    remote_delete('its.png')
    remote_list()
    remote_upload('its.png')
    remote_list()

