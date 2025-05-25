import argparse
import csv
import logging
import time
import os
import subprocess
import signal

import file_client_stresstest
from file_client_stresstest import run_test_batch, remote_list, FILENAME_MAP

FILE_SIZES_MB_REPORTING = {
    "10MB": 10,
    "50MB": 50,
    "100MB": 100,
}

def ensure_dummy_files():
    logging.info("Ensuring dummy files for upload tests exist...")
    for key, filename in FILENAME_MAP.items():
        if not os.path.exists(filename):
            size_mb = FILE_SIZES_MB_REPORTING.get(key)
            if size_mb:
                size_bytes = size_mb * 1024 * 1024
                logging.warning(f"Dummy file '{filename}' for {key} not found. Creating ({size_mb}MB)...")
                try:
                    with open(filename, 'wb') as f:
                        if size_bytes > 10*1024*1024: # For larger files, use seek for faster creation
                            f.seek(size_bytes -1)
                            f.write(b'\0')
                        else:
                            f.write(os.urandom(size_bytes))
                    logging.info(f"Created dummy file '{filename}' ({size_mb}MB).")
                except Exception as e:
                    logging.error(f"Failed to create dummy file '{filename}': {e}")
            else:
                logging.warning(f"No size mapping for key {key}, cannot create dummy file '{filename}'.")
        else:
            logging.info(f"Dummy file '{filename}' for {key} already exists.")


def start_server(server_script_name, ip, port, workers, log_level="INFO"):
    cmd = [
        "python3", server_script_name,
        "--ip", ip,
        "--port", str(port),
        "--workers", str(workers),
        "--loglevel", log_level
    ]
    logging.info(f"GridSearch: Starting server: {' '.join(cmd)}")

    server_stdout_log = f"server_{server_script_name.split('.')[0]}_w{workers}_p{port}.stdout.log"
    server_stderr_log = f"server_{server_script_name.split('.')[0]}_w{workers}_p{port}.stderr.log"

    try:
        stdout_file = open(server_stdout_log, 'w')
        stderr_file = open(server_stderr_log, 'w')

        server_process = subprocess.Popen(
            cmd,
            stdout=stdout_file,
            stderr=stderr_file, 
            preexec_fn=os.setsid if os.name != "nt" else None 
        )
        server_process.stdout_file = stdout_file
        server_process.stderr_file = stderr_file
        logging.info(f"GridSearch: Server '{server_script_name}' started. PID: {server_process.pid}. Logs: {server_stdout_log}, {server_stderr_log}")
        return server_process
    except FileNotFoundError:
        logging.error(f"GridSearch: Server script '{server_script_name}' not found. Ensure it's in PATH or CWD.")
        if 'stdout_file' in locals() and stdout_file: stdout_file.close()
        if 'stderr_file' in locals() and stderr_file: stderr_file.close()
        return None
    except Exception as e:
        logging.error(f"GridSearch: Failed to start server '{server_script_name}': {e}")
        if 'stdout_file' in locals() and stdout_file: stdout_file.close()
        if 'stderr_file' in locals() and stderr_file: stderr_file.close()
        return None

def check_server_readiness(server_process, server_ip, server_port, timeout_sec=30): # Increased timeout
    original_client_target_address = file_client_stresstest.server_address # Save for restoration
    file_client_stresstest.server_address = (server_ip, server_port)
    logging.info(f"GridSearch: Set client target for readiness check to {file_client_stresstest.server_address}")

    start_time = time.time()
    ready = False
    while time.time() - start_time < timeout_sec:
        connected, response = remote_list() 
        if connected:
            logging.info(f"GridSearch: Server is ready (responded to LIST: {response}).")
            ready = True
            break
        
        if server_process.poll() is not None:
            logging.error("GridSearch: Server process exited prematurely while checking readiness.")
            return False
        time.sleep(2)

    file_client_stresstest.server_address = original_client_target_address

    if not ready:
        logging.error(f"GridSearch: Server {server_ip}:{server_port} did not become ready within {timeout_sec} seconds.")
    return ready


def stop_server(server_process, timeout_sec=15): # Increased timeout
    if server_process is None or server_process.poll() is not None:
        if hasattr(server_process, 'stdout_file') and server_process.stdout_file:
            if not server_process.stdout_file.closed: server_process.stdout_file.close()
        if hasattr(server_process, 'stderr_file') and server_process.stderr_file:
            if not server_process.stderr_file.closed: server_process.stderr_file.close()
        return

    logging.info(f"GridSearch: Stopping server process (PID: {server_process.pid})...")
    try:
        if os.name == "nt":
            server_process.terminate() # Sends SIGTERM
        else:
            os.killpg(os.getpgid(server_process.pid), signal.SIGINT)

        server_process.wait(timeout=timeout_sec)
        logging.info(f"GridSearch: Server process (PID: {server_process.pid}) stopped (or was already stopped). Exit code: {server_process.returncode}")
    except subprocess.TimeoutExpired:
        logging.warning(f"GridSearch: Server (PID: {server_process.pid}) did not stop gracefully (SIGINT/TERM) within {timeout_sec}s. Terminating (SIGTERM/KILL)...")
        server_process.terminate()
        try:
            server_process.wait(timeout=5)
            logging.info(f"GridSearch: Server (PID: {server_process.pid}) terminated.")
        except subprocess.TimeoutExpired:
            logging.error(f"GridSearch: Server (PID: {server_process.pid}) did not terminate. Killing (SIGKILL)...")
            server_process.kill()
            server_process.wait()
            logging.info(f"GridSearch: Server (PID: {server_process.pid}) killed.")
    except Exception as e:
        logging.error(f"GridSearch: Error stopping server (PID: {server_process.pid}): {e}")
        if server_process.poll() is None:
            logging.warning(f"GridSearch: Forcing kill on server (PID: {server_process.pid}) due to prior error.")
            server_process.kill()
            server_process.wait()
    finally:
        if hasattr(server_process, 'stdout_file') and server_process.stdout_file:
            if not server_process.stdout_file.closed: server_process.stdout_file.close()
        if hasattr(server_process, 'stderr_file') and server_process.stderr_file:
            if not server_process.stderr_file.closed: server_process.stderr_file.close()


def main():
    parser = argparse.ArgumentParser(description="File Transfer Grid Search Stress Test Orchestrator")
    parser.add_argument('--server_ip', type=str, default='127.0.0.1', help='Server IP address for servers to bind and clients to target.')
    parser.add_argument('--server_port', type=int, default=6665, help='Base server port (will be incremented if multiple parallel orchestrators run).')

    parser.add_argument('--server_type_grid', type=str, default='mtpool,mppool', help='Comma-separated server types: mtpool, mppool')
    parser.add_argument('--operations_grid', type=str, default='download,upload', help='Comma-separated list: upload,download')
    parser.add_argument('--volumes_grid', type=str, default='10MB,50MB,100MB', help='Comma-separated keys from FILENAME_MAP: 10MB,50MB,100MB')
    parser.add_argument('--client_workers_grid', type=str, default='1,5,50', help='Comma-separated list of client worker pool sizes')
    parser.add_argument('--server_workers_grid', type=str, default='1,5,50', help='Comma-separated list of server worker pool sizes')
    
    parser.add_argument('--total_ops_per_config', type=int, default=1, help='Total operations (e.g., 10 uploads) for each specific client test configuration')
    parser.add_argument('--client_concurrency_mode', type=str, default='thread, process', choices=['thread', 'process'], help='Client concurrency mode for all tests in this run (thread or process)')
    
    parser.add_argument('--output_csv', type=str, default='stress_test_results_grid.csv', help='CSV file to store all results')
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    parser.add_argument('--server_startup_wait_max', type=int, default=30, help="Max seconds to wait for server readiness.")
    parser.add_argument('--pause_between_tests', type=int, default=3, help="Seconds to pause between client test combinations.")
    parser.add_argument('--pause_between_server_restarts', type=int, default=5, help="Seconds to pause after stopping a server before starting the next config.")


    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper()),
        format='%(asctime)s - %(levelname)s - GridOrchestrator - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    ensure_dummy_files()

    server_script_map = {
        "mtpool": "file_server_mtpool.py",
        "mppool": "file_server_mppool.py"
    }

    server_types_to_test = args.server_type_grid.split(',')
    operations_to_test = args.operations_grid.split(',')
    volumes_to_test = args.volumes_grid.split(',')
    client_workers_list = [int(x) for x in args.client_workers_grid.split(',')]
    server_workers_config_list = [int(x) for x in args.server_workers_grid.split(',')]

    all_run_results = []
    test_run_counter = 0
    current_server_process = None
    
    logging.info(f"Starting grid search. Client Concurrency Mode for this run: {args.client_concurrency_mode}. Total ops per config: {args.total_ops_per_config}")
    
    try:
        for server_type_key in server_types_to_test:
            server_script = server_script_map.get(server_type_key)
            if not server_script:
                logging.error(f"GridSearch: Unknown server type '{server_type_key}'. Skipping.")
                continue

            for num_server_workers in server_workers_config_list:
                logging.info(f"----- Preparing for Server Config: Type={server_type_key}, ServerWorkers={num_server_workers} -----")
                
                if current_server_process and current_server_process.poll() is None:
                    logging.warning("GridSearch: Found an existing server process. Attempting to stop it before starting a new one.")
                    stop_server(current_server_process, args.server_startup_wait_max // 2)
                    time.sleep(2)

                current_server_process = start_server(
                    server_script, args.server_ip, args.server_port, num_server_workers, args.loglevel
                )
                if not current_server_process:
                    logging.error(f"GridSearch: Failed to start server {server_script} with {num_server_workers} workers. Skipping this server config.")
                    continue
                
                time.sleep(2)
                if not check_server_readiness(current_server_process, args.server_ip, args.server_port, args.server_startup_wait_max):
                    logging.error(f"GridSearch: Server {server_script} (workers={num_server_workers}) failed to become ready. Stopping it and skipping.")
                    stop_server(current_server_process)
                    current_server_process = None
                    logging.info(f"GridSearch: Pausing for {args.pause_between_server_restarts}s after failed server start...")
                    time.sleep(args.pause_between_server_restarts)
                    continue
                
                logging.info(f"GridSearch: Server {server_script} (workers={num_server_workers}) is UP on {args.server_ip}:{args.server_port}. Proceeding with client tests.")

                for op_type in operations_to_test:
                    for vol_key in volumes_to_test:
                        if vol_key not in FILENAME_MAP:
                            logging.warning(f"GridSearch: Skipping volume '{vol_key}' as it's not in FILENAME_MAP.")
                            continue

                        if op_type == 'upload':
                            expected_local_file = FILENAME_MAP[vol_key]
                            if not os.path.exists(expected_local_file):
                                logging.error(
                                    f"GridSearch: PRE-FLIGHT CHECK FAILED for UPLOAD of {vol_key}: "
                                    f"Source file '{expected_local_file}' not found in CWD ({os.getcwd()}). "
                                    f"Skipping client worker iterations for this specific config (Op: {op_type}, Vol: {vol_key})."
                                )
                                continue
                        
                        for num_client_w in client_workers_list:
                            test_run_counter += 1
                            logging.info(f"--- Grid Test Run ID: {test_run_counter} ---")
                            logging.info(
                                f"Config: ServerType={server_type_key}, ServerWorkers={num_server_workers}, Op={op_type}, Vol={vol_key}, "
                                f"ClientWorkers={num_client_w}, TotalOpsBatch={args.total_ops_per_config}, ClientMode={args.client_concurrency_mode}"
                            )
                            
                            batch_summary = run_test_batch(
                                p_server_ip=args.server_ip,
                                p_server_port=args.server_port,
                                p_action=op_type,
                                p_file_key=vol_key,
                                p_num_client_workers=num_client_w,
                                p_total_ops=num_client_w,
                                p_client_pool_mode=args.client_concurrency_mode
                            )

                            throughput_MBps = batch_summary['avg_op_throughput_Bps'] / (1024 * 1024) if batch_summary['avg_op_throughput_Bps'] is not None else 0.0
                            avg_op_duration = batch_summary['avg_op_duration_s'] if batch_summary['avg_op_duration_s'] is not None else 0.0

                            row = {
                                "Nomor": test_run_counter,
                                "Server Type": server_type_key, 
                                "Operasi": op_type,
                                "Volume (MB)": FILE_SIZES_MB_REPORTING.get(vol_key, "N/A"),
                                "Client Concurrency Mode": args.client_concurrency_mode,
                                "Jumlah client worker pool": num_client_w,
                                "Jumlah server worker pool": num_server_workers,
                                "Waktu total per client (avg s)": f"{avg_op_duration:.4f}",
                                "Throughput per client (avg MBps)": f"{throughput_MBps:.4f}",
                                "Jumlah worker client yang sukses": batch_summary['ops_successful'],
                                "Jumlah worker client yang gagal": batch_summary['ops_failed'],
                                "Jumlah worker server yang sukses": batch_summary['ops_successful'], 
                                "Jumlah worker server yang gagal": batch_summary['ops_failed'],
                                "Batch Wall Time (s)": f"{batch_summary['batch_wall_time_s']:.2f}",
                            }
                            all_run_results.append(row)
                            
                            logging.info(f"Result ID {test_run_counter}: Success={batch_summary['ops_successful']}/{args.total_ops_per_config}, AvgClientTime={avg_op_duration:.4f}s, AvgClientThr={throughput_MBps:.4f}MBps")
                            logging.info(f"GridSearch: Pausing for {args.pause_between_tests} seconds before next client test combination...")
                            time.sleep(args.pause_between_tests)
                
                logging.info(f"----- Finished tests for Server Config: Type={server_type_key}, Workers={num_server_workers} -----")
                stop_server(current_server_process)
                current_server_process = None
                logging.info(f"GridSearch: Waiting {args.pause_between_server_restarts} seconds for server to fully release resources...")
                time.sleep(args.pause_between_server_restarts)

    except KeyboardInterrupt:
        logging.warning("GridSearch: Orchestrator interrupted by user. Shutting down any running server...")
    except Exception as e:
        logging.error(f"GridSearch: Orchestrator encountered an unhandled exception: {e}", exc_info=True)
    finally:
        if current_server_process and current_server_process.poll() is None:
            logging.info("GridSearch: Orchestrator exiting. Ensuring server is stopped.")
            stop_server(current_server_process)

    if all_run_results:
        field_names = [
            "Nomor", "Server Type", "Operasi", "Volume (MB)", 
            "Client Concurrency Mode", "Jumlah client worker pool", "Jumlah server worker pool",
            "Waktu total per client (avg s)", "Throughput per client (avg MBps)",
            "Jumlah worker client yang sukses", "Jumlah worker client yang gagal",
            "Jumlah worker server yang sukses", "Jumlah worker server yang gagal",
            "Batch Wall Time (s)"
        ]
        if not all(fn in all_run_results[0] for fn in field_names):
            logging.error("CSV header mismatch! Generated headers do not match all data keys in results.")
            logging.error(f"Expected headers (from field_names list): {field_names}")
            logging.error(f"Actual keys in first data row: {list(all_run_results[0].keys())}")

        try:
            with open(args.output_csv, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names, extrasaction='ignore') # Ignore extra fields if any
                writer.writeheader()
                writer.writerows(all_run_results)
            logging.info(f"Grid search stress test results successfully written to {args.output_csv}")
        except Exception as e:
            logging.error(f"GridSearch: Failed to write results to CSV {args.output_csv}: {e}")
    else:
        logging.info("GridSearch: No test results were generated to write to CSV.")

if __name__ == '__main__':
    main()