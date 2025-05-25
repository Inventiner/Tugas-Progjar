import os
import json
import base64
from glob import glob

class FileInterface:
    def __init__(self, base_storage_path="files"):
        self.storage_dir = os.path.abspath(base_storage_path)
        if not os.path.exists(self.storage_dir):
            try:
                os.makedirs(self.storage_dir)
            except OSError as e:
                raise 

    def _get_full_path(self, filename):
        if os.path.isabs(filename) or ".." in filename:
             return None
        return os.path.join(self.storage_dir, filename)

    def list(self, params=[]):
        try:
            filelist = [os.path.basename(f) for f in glob(os.path.join(self.storage_dir, '*.*'))]
            return dict(status='OK', data=filelist)
        except Exception as e:
            return dict(status='ERROR', data=str(e))

    def get(self, params=[]):
        try:
            if not params:
                return dict(status='ERROR', data='Filename not provided for GET')
            filename = params[0]
            if not filename:
                return dict(status='ERROR', data='Filename cannot be empty for GET')

            full_path = self._get_full_path(filename)
            if not full_path:
                return dict(status='ERROR', data=f"Invalid filename '{filename}' for GET.")
            
            with open(full_path, 'rb') as fp:
                isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK', data_namafile=filename, data_file=isifile)
        except FileNotFoundError:
            return dict(status='ERROR', data=f"File '{filename}' not found.")
        except Exception as e:
            return dict(status='ERROR', data=str(e))

    def upload(self, params=[]):
        try:
            if len(params) < 2:
                return dict(status='ERROR', data='UPLOAD command requires filename and content.')
            filename = params[0]
            content_b64 = params[1]
            if not filename or not content_b64:
                return dict(status='ERROR', data='Filename or content cannot be empty for UPLOAD.')

            full_path = self._get_full_path(filename)
            if not full_path:
                return dict(status='ERROR', data=f"Invalid filename '{filename}' for UPLOAD.")

            with open(full_path, 'wb+') as fp:
                fp.write(base64.b64decode(content_b64.encode()))
            return dict(status='OK', data=f"File '{filename}' uploaded successfully to {self.storage_dir}.")
        except base64.binascii.Error as b64e:
            return dict(status='ERROR', data=f"Invalid Base64 content for UPLOAD: {str(b64e)}")
        except Exception as e:
            return dict(status='ERROR', data=str(e))

    def delete(self, params=[]):
        try:
            if not params:
                return dict(status='ERROR', data='Filename not provided for DELETE')
            filename = params[0]
            if not filename:
                return dict(status='ERROR', data='Filename cannot be empty for DELETE')

            full_path = self._get_full_path(filename)
            if not full_path:
                return dict(status='ERROR', data=f"Invalid filename '{filename}' for DELETE.")

            if os.path.exists(full_path):
                os.remove(full_path)
                return dict(status='OK', data=f"File '{filename}' deleted successfully from {self.storage_dir}.")
            else:
                return dict(status='ERROR', data=f"File '{filename}' not found for deletion.")
        except Exception as e:
            return dict(status='ERROR', data=str(e))

if __name__=='__main__':
    if not os.path.exists("pokijan.jpg"):
        with open("pokijan.jpg", "wb") as f:
            f.write(b"dummy content for pokijan")

    f = FileInterface()
    print("--- LIST ---")
    print(f.list())
    
    print("\n--- GET pokijan.jpg ---")
    print(f.get(['pokijan.jpg']))
    
    print("\n--- GET non_existent_file.txt ---")
    print(f.get(['non_existent_file.txt']))

    print("\n--- UPLOAD test_upload.txt ---")
    dummy_content_b64 = base64.b64encode(b"Hello from upload test").decode()
    print(f.upload(['test_upload.txt', dummy_content_b64]))
    print(f.list())

    print("\n--- DELETE test_upload.txt ---")
    print(f.delete(['test_upload.txt']))
    print(f.list())
    
    if os.path.exists("pokijan.jpg"):
        os.remove("pokijan.jpg")
    
    os.chdir('..')