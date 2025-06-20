import json
import logging
import shlex

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
MAX_LOG_LEN = 100

class FileProtocol:
    def __init__(self):
        self.file = FileInterface()
    def proses_string(self,string_datamasuk=''):
        log_display_string = string_datamasuk
        if len(string_datamasuk) > MAX_LOG_LEN:
            log_display_string = string_datamasuk[:MAX_LOG_LEN] + f"... [Truncated, Total len: {len(string_datamasuk)}]"
        else:
            log_display_string = string_datamasuk
            
        logging.warning(f"string diproses: {log_display_string}")
        c = shlex.split(string_datamasuk)
        if not c:
            return json.dumps(dict(status='ERROR', data='Empty request received'))
        try:
            c_request = c[0].lower().strip()
            logging.warning(f"memproses request: {c_request}")
            params = [x for x in c[1:]]
            cl = getattr(self.file,c_request)(params)
            return json.dumps(cl)
        except Exception:
            return json.dumps(dict(status='ERROR',data='request tidak dikenali'))


if __name__=='__main__':
    #contoh pemakaian
    fp = FileProtocol()
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET pokijan.jpg"))
