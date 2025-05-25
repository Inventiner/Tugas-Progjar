import socket
import json
import base64
import logging
import time
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

server_address = ('0.0.0.0', 6665)

FILENAME_MAP = {
    "10MB": "10m.jpg",
    "50MB": "50m.mp4",
    "100MB": "100m.mp4"
}

def truncate_data(data, max_len=100):
    s_data = str(data)
    if len(s_data) > max_len:
        half_len = max(1, max_len // 2)
        return s_data[:half_len] + "..." + s_data[-half_len:] + f" (len {len(s_data)})"
    return s_data

def send_command(command_str=""):
    global server_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    timeout_duration = 300 
    sock.settimeout(timeout_duration)
    
    try:
        sock.connect(server_address)

        command_to_send = command_str + "\r\n\r\n"
        
        sock.sendall(command_to_send.encode())

        data_received = ""
        while True:
            data = sock.recv(8192 * 2)
            if data:
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    message_part = data_received.split("\r\n\r\n", 1)[0]
                    try:
                        hasil = json.loads(message_part)
                    except json.JSONDecodeError as je:
                        logging.error(f"JSONDecodeError from server: {je}. Received: {truncate_data(message_part)}")
                        return dict(status='ERROR', data=f"JSONDecodeError: {je}")
                    return hasil
            else:
                logging.warning(f"Socket closed by server prematurely for command {command_str.split(' ')[0]}. Received so far: {truncate_data(data_received)}")
                return dict(status='ERROR', data='Connection closed prematurely by server')
    except socket.timeout:
        logging.error(f"Socket timeout connecting or receiving from {server_address} for command {command_str.split(' ')[0]}")
        return dict(status='ERROR', data=f'Socket timeout after {timeout_duration}s')
    except ConnectionRefusedError:
        logging.error(f"Connection refused by {server_address} for command {command_str.split(' ')[0]}")
        return dict(status='ERROR',data=f'Connection refused by server {server_address}')
    except Exception as e:
        logging.error(f"Error in send_command for {command_str.split(' ')[0]}: {e}", exc_info=False) # exc_info can be verbose
        return dict(status='ERROR',data=str(e))
    finally:
        sock.close()

def remote_list():
    command_str = "LIST"
    hasil = send_command(command_str)
    if hasil.get('status') == 'OK':
        return True, hasil
    else:
        logging.error(f"Gagal LIST: {hasil.get('data', 'Unknown error')}")
        return False, hasil

def remote_get(filename=""):
    if not filename:
        return False, {"status": "ERROR", "data": "Filename for GET not provided"}, 0
        
    command_str = f"GET {filename}"
    hasil = send_command(command_str)
    bytes_dl = 0
    if hasil.get('status') == 'OK':
        try:
            isifile_b64 = hasil.get('data_file', '')
            if isifile_b64:
                decoded_bytes = base64.b64decode(isifile_b64)
                bytes_dl = len(decoded_bytes)
            else:
                logging.warning(f"GET '{filename}' status OK, but no 'data_file' field or it's empty in response.")
            return True, hasil, bytes_dl
        except base64.binascii.Error as b64e:
            logging.error(f"Base64 decode error for GET '{filename}': {b64e}")
            return False, {"status": "ERROR", "data": f"Base64 decode error: {b64e}"}, 0
        except Exception as e:
            logging.error(f"Error processing GET response for '{filename}': {e}")
            return False, {"status": "ERROR", "data": f"Client-side error processing GET: {e}"}, 0
    else:
        logging.error(f"Gagal GET '{filename}': {hasil.get('data', 'Unknown error')}")
        return False, hasil, 0

def remote_upload(filename_local_and_remote=""):
    if not filename_local_and_remote:
        logging.error("UPLOAD call missing filename.")
        return False, {"status": "ERROR", "data": "Filename for upload not provided"}, 0

    if not os.path.exists(filename_local_and_remote):
        logging.error(f"Local file '{filename_local_and_remote}' not found for upload.")
        return False, {"status": "ERROR", "data": f"Local file '{filename_local_and_remote}' not found"}, 0
    
    bytes_ul = 0
    try:
        bytes_ul = os.path.getsize(filename_local_and_remote)
        with open(filename_local_and_remote, 'rb') as fp:
            file_content_bytes = fp.read()
            
        isifile_b64 = base64.b64encode(file_content_bytes).decode()
        command_str = f"UPLOAD {filename_local_and_remote} {isifile_b64}"
        hasil = send_command(command_str)

        if hasil.get('status') == 'OK':
            return True, hasil, bytes_ul
        else:
            logging.error(f"Gagal UPLOAD '{filename_local_and_remote}': {hasil.get('data', 'Unknown error')}")
            return False, hasil, 0
        
    except Exception as e:
        logging.error(f"Exception during remote_upload of '{filename_local_and_remote}': {e}", exc_info=False)
        return False, {"status": "ERROR", "data": f"Client-side exception during upload: {e}"}, 0

def remote_delete(filename=""):
    if not filename:
        logging.error("DELETE call missing filename.")
        return False, {"status": "ERROR", "data": "Filename for DELETE not provided"}
        
    command_str = f"DELETE {filename}"
    hasil = send_command(command_str)
    if hasil.get('status') == 'OK':
        return True, hasil
    else:
        logging.error(f"Gagal DELETE '{filename}': {hasil.get('data', 'Unknown error')}")
        return False, hasil

def client_single_op_runner(action, file_key):
    start_time = time.perf_counter()
    success = False
    bytes_transferred = 0
    
    filename_to_use = FILENAME_MAP[file_key]

    if action == "upload":
        success, _, bytes_transferred = remote_upload(filename_to_use)
    elif action == "download":
        success, _, bytes_transferred = remote_get(filename_to_use)
    
    duration_sec = time.perf_counter() - start_time
    
    return {
        "success": success,
        "duration_sec": duration_sec,
        "bytes_transferred": bytes_transferred if success else 0,
    }


def run_test_batch(
        p_server_ip, p_server_port,
        p_action, p_file_key,
        p_num_client_workers, p_total_ops,
        p_client_pool_mode):
    
    global server_address
    server_address = (p_server_ip, p_server_port)

    ExecutorClass = ThreadPoolExecutor if p_client_pool_mode == 'thread' else ProcessPoolExecutor
    
    logging.info(
        f"Starting Batch: TargetServer={server_address}, Action={p_action}, FileKey={p_file_key}, "
        f"ClientWorkers={p_num_client_workers}, TotalOps={p_total_ops}, ClientMode={p_client_pool_mode}"
    )

    op_results_list = []
    batch_start_time = time.perf_counter()

    if p_action == 'upload':
        local_file_for_upload = FILENAME_MAP[p_file_key]
        if not os.path.exists(local_file_for_upload):
            logging.error(
                f"PRE-BATCH CHECK FAILED for UPLOAD of {p_file_key}: "
                f"Source file '{local_file_for_upload}' not found in CWD ({os.getcwd()}). "
                f"This batch will likely have all ops fail."
            )

    with ExecutorClass(max_workers=p_num_client_workers) as executor:
        futures = [executor.submit(client_single_op_runner, p_action, p_file_key) for _ in range(p_total_ops)]
        
        for i, future in enumerate(as_completed(futures)):
            try:
                result = future.result()
                op_results_list.append(result)
            except Exception as e:
                logging.error(f"Exception from a client task future: {e}", exc_info=True)
                op_results_list.append({"success": False, "duration_sec": 0, "bytes_transferred": 0})
    
    batch_wall_time_s = time.perf_counter() - batch_start_time

    successful_ops_count = sum(1 for r in op_results_list if r["success"])
    failed_ops_count = len(op_results_list) - successful_ops_count
    
    total_duration_successful_s = sum(r['duration_sec'] for r in op_results_list if r["success"])
    total_bytes_successful = sum(r['bytes_transferred'] for r in op_results_list if r["success"])
    
    avg_op_duration_s = total_duration_successful_s / successful_ops_count if successful_ops_count > 0 else 0
    avg_op_throughput_Bps = total_bytes_successful / total_duration_successful_s if total_duration_successful_s > 0 else 0

    logging.info(
        f"Batch Finished. WallTime={batch_wall_time_s:.2f}s. SuccessOps={successful_ops_count}, FailedOps={failed_ops_count}. "
        f"AvgOpDur_Success={avg_op_duration_s:.4f}s, AvgOpThr_Success={avg_op_throughput_Bps / (1024*1024):.4f} MB/s"
    )
    
    return {
        "avg_op_duration_s": avg_op_duration_s,
        "avg_op_throughput_Bps": avg_op_throughput_Bps,
        "ops_successful": successful_ops_count,
        "ops_failed": failed_ops_count,
        "batch_wall_time_s": batch_wall_time_s,
        "total_bytes_transferred_successful_ops": total_bytes_successful,
    }


def main():
    parser = argparse.ArgumentParser(description="Client Stress Tester - Single Batch Runner")
    parser.add_argument('--server_ip', type=str, default='127.0.0.1', help='Server IP address')
    parser.add_argument('--server_port', type=int, default=6665, help='Server port')
    parser.add_argument('--workers', type=int, default=1, help='Number of client worker threads/processes for this batch')
    parser.add_argument('--total_ops', type=int, default=1, help='Total number of operations for this batch')
    parser.add_argument('--mode', type=str, default='thread', choices=['thread', 'process'], help='Client concurrency mode')
    parser.add_argument('--action', type=str, required=True, choices=['upload', 'download', 'list'], help='Action to perform')
    parser.add_argument('--file_key', type=str, default=list(FILENAME_MAP.keys())[0], choices=list(FILENAME_MAP.keys()), help='File key (e.g., 10MB)')
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    
    args = parser.parse_args()
    
    global server_address 
    server_address = (args.server_ip, args.server_port)

    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper()),
        format='%(asctime)s - %(levelname)s - %(processName)s-%(threadName)s - ClientStress - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info(f"STRESS_CLIENT (Single Batch): Target server set to {server_address}")
    
    if args.action == 'upload':
        local_file_path = FILENAME_MAP[args.file_key]
        if not os.path.exists(local_file_path):
            logging.critical(f"Required file for upload '{local_file_path}' (key: {args.file_key}) not found in CWD: {os.getcwd()}. Please create it.")
            file_size_bytes = 0
            if args.file_key == "10MB": file_size_bytes = 10 * 1024 * 1024
            elif args.file_key == "50MB": file_size_bytes = 50 * 1024 * 1024
            elif args.file_key == "100MB": file_size_bytes = 100 * 1024 * 1024
            
            if file_size_bytes > 0:
                logging.warning(f"Creating dummy file {local_file_path} of size {file_size_bytes} bytes.")
                with open(local_file_path, 'wb') as f:
                    f.write(os.urandom(file_size_bytes))
            else:
                 return 

    results = run_test_batch(
        p_server_ip=args.server_ip,
        p_server_port=args.server_port,
        p_action=args.action,
        p_file_key=args.file_key,
        p_num_client_workers=args.workers,
        p_total_ops=args.total_ops,
        p_client_pool_mode=args.mode
    )

    logging.info(f"CLI Batch Test Results for {args.action} {args.file_key} (Client Mode: {args.mode}):")
    logging.info(f"  Avg Op Duration (successful ops): {results['avg_op_duration_s']:.4f} s")
    logging.info(f"  Avg Op Throughput (successful ops): {results['avg_op_throughput_Bps'] / (1024*1024):.4f} MB/s")
    logging.info(f"  Successful Ops: {results['ops_successful']}")
    logging.info(f"  Failed Ops: {results['ops_failed']}")
    logging.info(f"  Total Bytes Transferred (successful ops): {results['total_bytes_transferred_successful_ops']} B")
    logging.info(f"  Batch Wall Time: {results['batch_wall_time_s']:.2f} s")
            
if __name__=='__main__':
    main()