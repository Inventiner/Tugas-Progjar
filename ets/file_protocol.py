import json
import logging

from file_interface import FileInterface

"""
* class FileProtocol bertugas untuk memproses 
data yang masuk, dan menerjemahkannya apakah sesuai dengan
protokol/aturan yang dibuat

* data yang masuk dari client adalah dalam bentuk bytes yang 
pada akhirnya akan diproses dalam bentuk string

* class FileProtocol akan memproses data yang masuk dalam bentuk
string
"""
MAX_LOG_LEN = 200

class FileProtocol:
    def __init__(self):
        self.file = FileInterface()

    def proses_string(self,string_datamasuk=''):
        log_display_string = string_datamasuk
        if len(string_datamasuk) > MAX_LOG_LEN:
            command_part = string_datamasuk.split(' ')[0]
            if command_part.upper() == "UPLOAD" and len(string_datamasuk.split(' ')) > 2:
                 filename_part = string_datamasuk.split(' ')[1]
                 log_display_string = f"{command_part} {filename_part} [CONTENT_TRUNCATED]... (Total len: {len(string_datamasuk)})"
            else:
                log_display_string = string_datamasuk[:MAX_LOG_LEN] + f"... [Truncated, Total len: {len(string_datamasuk)}]"
        
        c = str.split(string_datamasuk)
        if not c:
            logging.warning("Server Proto: Empty request received.")
            return json.dumps(dict(status='ERROR', data='Empty request received'))
        
        try:
            c_request = c[0].strip().lower()
            params = [x for x in c[1:]]
            
            if hasattr(self.file, c_request):
                method_to_call = getattr(self.file, c_request)
                cl = method_to_call(params)
                return json.dumps(cl)
            else:
                logging.warning(f"Server Proto: Unknown command '{c_request}'. Full request: {log_display_string}")
                return json.dumps(dict(status='ERROR',data=f"Request command '{c_request}' not recognized"))
        except AttributeError:
             logging.error(f"Server Proto: Attribute error for command. Request: {log_display_string}", exc_info=True)
             return json.dumps(dict(status='ERROR',data='Internal server error processing request type'))
        except Exception as e:
            logging.error(f"Server Proto: Exception processing string '{log_display_string}': {e}", exc_info=True)
            return json.dumps(dict(status='ERROR',data=f'Error processing request: {str(e)}'))


if __name__=='__main__':
    if not os.path.exists('files'):
        os.makedirs('files')
        print("Created 'files' directory for FileProtocol test.")

    fp = FileProtocol()
    print("--- LIST ---")
    print(fp.proses_string("LIST"))
    
    with open("files/pokijan.jpg", "wb") as f_dummy:
        f_dummy.write(base64.b64decode("R0lGODlhAQABAIAAAAUEBAAAACwAAAAAAQABAAACAkQBADs="))
    print("\n--- GET pokijan.jpg ---")
    print(fp.proses_string("GET pokijan.jpg"))

    print("\n--- GET non_existent_file.txt ---")
    print(fp.proses_string("GET non_existent_file.txt"))

    print("\n--- UPLOAD test_upload.txt ---")
    dummy_content_b64 = base64.b64encode(b"Hello from FileProtocol upload test").decode()
    print(fp.proses_string(f"UPLOAD test_upload_protocol.txt {dummy_content_b64}"))
    print(fp.proses_string("LIST")) 

    print("\n--- DELETE test_upload_protocol.txt ---")
    print(fp.proses_string("DELETE test_upload_protocol.txt"))
    print(fp.proses_string("LIST"))
    
    print("\n--- UNKNOWN COMMAND ---")
    print(fp.proses_string("DOESNOTEXIST some params"))

    print("\n--- EMPTY COMMAND ---")
    print(fp.proses_string(""))
    print(fp.proses_string("   "))

    if os.path.exists("files/pokijan.jpg"):
        os.remove("files/pokijan.jpg")
    if os.path.exists("files/test_upload_protocol.txt"):
        os.remove("files/test_upload_protocol.txt")