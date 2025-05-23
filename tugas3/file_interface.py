import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
        os.chdir('files/')

    def list(self,params=[]):
        try:
            filelist = glob('*.*')
            return dict(status='OK',data=filelist)
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def get(self,params=[]):
        try:
            filename = params[0]
            if (filename == ''):
                return None
            fp = open(f"{filename}",'rb')
            isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK',data_namafile=filename,data_file=isifile)
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def upload(self, params=[]):
        try:
            if len(params) < 2:
                return dict(status='ERROR', data='UPLOAD command requires filename and content.')
            filename = params[0]
            content = params[1]
            if not filename or not content:
                return dict(status='ERROR', data='Filename or content cannot be empty for UPLOAD.')
            with open(filename, 'wb+') as fp:
                fp.write(base64.b64decode(content.encode()))
            return dict(status='OK', data=f"File '{filename}' uploaded successfully.")
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def delete(self, params=[]):
        try:
            if not params:
                return dict(status='ERROR', data='Filename not provided for DELETE')
            filename = params[0]
            if not filename:
                return dict(status='ERROR', data='Filename cannot be empty for DELETE')

            if os.path.exists(filename):
                os.remove(filename)
                return dict(status='OK', data=f"File '{filename}' deleted successfully.")
            else:
                return dict(status='ERROR', data=f"File '{filename}' not found for deletion.")
        except Exception as e:
            return dict(status='ERROR', data=str(e))

if __name__=='__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))
