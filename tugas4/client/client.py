import socket
import json
import os
import base64

def send_request(request_data, host='localhost', port=8885):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_address = (host, port)
        sock.connect(server_address)
        sock.sendall(request_data)

        response_raw = b''
        while True:
            data = sock.recv(1024)
            if not data:
                break
            response_raw += data
        
        response_str = response_raw.decode('utf-8', errors='ignore')
        
        try:
            headers, body = response_str.split('\r\n\r\n', 1)
        except ValueError:
            headers = response_str
            body = ""

        print("\n--- Respons dari Server ---")
        print(headers)
        print("---------------------------\nBody:")
        try:
            parsed_json = json.loads(body)
            print(json.dumps(parsed_json, indent=4))
        except (json.JSONDecodeError, TypeError):
            print(body.strip())
        
        print("---------------------------")
    except Exception as e:
        print(f"\n[ERROR] Terjadi kesalahan: {e}")
    finally:
        sock.close()

def get_file_list(host='localhost', port=8885):
    print("\n[INFO] Meminta daftar file dari endpoint /list...")
    request = b"GET /list HTTP/1.0\r\n\r\n\r\n"
    send_request(request, host, port)

def upload_file(filepath, host='localhost', port=8885):
    if not os.path.exists(filepath):
        print(f"\n[ERROR] File lokal '{filepath}' tidak ditemukan.")
        return
        
    print(f"\n[INFO] Mengirim file '{filepath}' ke endpoint /upload...")

    with open(filepath, 'rb') as f:
        binary_content = f.read()

    base64_bytes = base64.b64encode(binary_content)
    base64_string = base64_bytes.decode('utf-8')

    body_content = base64_string.encode('utf-8')
    
    filename = os.path.basename(filepath)

    request_data = (
        f"POST /upload HTTP/1.0\r\n"
        f"X-Filename: {filename}\r\n"
        f"Content-Length: {len(body_content)}\r\n"
        f"Content-Transfer-Encoding: base64\r\n"
        f"\r\n"
    ).encode('utf-8') + body_content + b'\r\n\r\n\r\n'

    send_request(request_data, host, port)

def delete_file(filename, host='localhost', port=8885):
    print(f"\n[INFO] Mengirim permintaan hapus untuk '{filename}' ke endpoint /delete/{filename}...")    
    request = f"DELETE /delete/{filename} HTTP/1.0\r\n\r\n\r\n".encode('utf-8')
    send_request(request, host, port)
    
if __name__ == '__main__':
    # 8885 untuk Thread Pool Server (default)
    # 8889 untuk Process Pool Server
    HOST='localhost'
    PORT = 8889

    get_file_list(host=HOST, port=PORT)
    upload_file('domain.crt', host=HOST, port=PORT)
    get_file_list(host=HOST, port=PORT)
    delete_file('domain.crt', host=HOST, port=PORT)
    get_file_list(host=HOST, port=PORT)
    upload_file('Prak2.jpg', host=HOST, port=PORT)