from socket import *
import socket
import threading
import logging
import time
import sys
import argparse
import os # For os.cpu_count()
from concurrent.futures import ThreadPoolExecutor

# Assuming file_protocol.py is in the same directory or Python path
from file_protocol import FileProtocol
# fp_global = FileProtocol() # Instantiate once if FileInterface's os.chdir() is managed carefully
# However, os.chdir in FileInterface.__init__ makes it tricky for a single global FileProtocol
# if the server's CWD is important for other things.
# Better to have FileProtocol instantiated where os.chdir is safe, e.g. if server CWD is fixed.
# For ThreadPool, since all threads share the same CWD, one fp_global could work if FileInterface
# is designed such that os.chdir('files') is called once and is stable.

class ProcessTheClient(): # Removed (threading.Thread) as it's now a target for pool threads
    def __init__(self, connection, address, fp_protocol_instance):
        self.connection = connection
        self.address = address
        self.fp_protocol = fp_protocol_instance

    def run(self):
        buffer = ""
        try:
            self.connection.settimeout(120) # Timeout for individual connection operations
            while True:
                data = self.connection.recv(16384) # Increased buffer size
                if not data: # Connection closed by client
                    logging.info(f"Connection closed by {self.address}")
                    break
                try:
                    buffer += data.decode() # Assuming UTF-8
                except UnicodeDecodeError as e:
                    logging.error(f"UnicodeDecodeError from {self.address}: {e}. Buffer: {buffer[:100]}...")
                    break
                
                while "\r\n\r\n" in buffer:
                    command_to_process, buffer = buffer.split("\r\n\r\n", 1)
                    # logging.debug(f"Processing command from {self.address}: {command_to_process.split(' ')[0]}")
                    
                    hasil_json_str = self.fp_protocol.proses_string(command_to_process)
                    
                    response_to_send = hasil_json_str + "\r\n\r\n"
                    self.connection.sendall(response_to_send.encode())
                    # logging.debug(f"Sent response to {self.address} for {command_to_process.split(' ')[0]}")

        except socket.timeout:
            logging.warning(f"Socket timeout for client {self.address}.")
        except ConnectionResetError:
            logging.warning(f"Connection reset by client {self.address}.")
        except BrokenPipeError:
            logging.warning(f"Broken pipe with client {self.address}.")
        except Exception as e:
            # Log full traceback for unexpected errors in worker threads
            logging.error(f"Unexpected error processing client {self.address}: {e}", exc_info=True)
        finally:
            self.connection.close()
            logging.info(f"Connection with {self.address} ended.")


class Server(threading.Thread):
    def __init__(self,ipaddress='0.0.0.0',port=6665, max_workers=None): # Default port changed
        super().__init__()
        self.ipinfo=(ipaddress,port)
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if max_workers is None or max_workers <= 0:
            # For ThreadPool, a higher default might be acceptable than CPU cores, e.g., 2 * os.cpu_count()
            # But let's stick to os.cpu_count() or a fixed sensible default like 10-20 for simplicity here.
            # The assignment specifies 1, 5, 50 as test values, so default can be one of these.
            self.max_workers = (os.cpu_count() or 1) * 5 # A common default for I/O bound tasks
            logging.info(f"Max workers not specified or invalid, defaulting to {self.max_workers}")
        else:
            self.max_workers = max_workers
            
        self.shutdown_event = threading.Event()
        self.executor = None # ThreadPoolExecutor

        # With os.chdir('files') in FileInterface, having one FileProtocol instance
        # for all threads is generally fine because all threads share the CWD.
        # The FileInterface methods are then operating within that 'files' dir.
        self.fp_protocol_main_instance = FileProtocol()


    # This method will be the target for executor.submit
    def process_connection_task(self, connection, address):
        # Each task (client connection) uses the shared fp_protocol_main_instance
        client_processor = ProcessTheClient(connection, address, self.fp_protocol_main_instance)
        client_processor.run()
    
    def run(self):
        logging.warning(f"MTPool Server starting on {self.ipinfo}, max worker threads: {self.max_workers}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(128) # Increased backlog
        self.my_socket.settimeout(1.0) # For non-blocking accept to check shutdown_event

        try:
            # Context manager for ThreadPoolExecutor ensures shutdown
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                self.executor = executor
                logging.info(f"ThreadPoolExecutor started with up to {self.max_workers} worker threads.")
                
                while not self.shutdown_event.is_set():
                    try:
                        connection, client_address = self.my_socket.accept()
                        logging.info(f"MainThread: Accepted connection from {client_address}")
                        # Submit the client processing task to the executor
                        self.executor.submit(self.process_connection_task, connection, client_address)
                    except socket.timeout:
                        continue # To check shutdown_event
                    except OSError as e:
                        if self.shutdown_event.is_set():
                            logging.info("MT Server: Socket operation interrupted by shutdown.")
                            break
                        logging.error(f"MT Server: Socket error during accept: {e}", exc_info=True)
                        break
        except KeyboardInterrupt:
            logging.warning("MT Server: KeyboardInterrupt received, initiating shutdown...")
        except Exception as e:
            logging.error(f"MT Server: Main loop encountered an unhandled error: {e}", exc_info=True)
        finally:
            self.shutdown_event.set()
            logging.warning("MT Server: Shutdown initiated.")

            # The 'with ThreadPoolExecutor...' context manager handles executor.shutdown(wait=True)
            if self.executor and not self.executor._shutdown:
                 logging.info("MT Server: Explicitly shutting down ThreadPoolExecutor.")
                 self.executor.shutdown(wait=True) # Ensure threads complete ongoing tasks

            self.my_socket.close()
            logging.warning("MT Server: Listening socket closed. Shutdown complete.")


    def stop(self):
        logging.info("MT Server: Stop requested.")
        self.shutdown_event.set()

def main():
    parser = argparse.ArgumentParser(description="File Server with ThreadPoolExecutor")
    parser.add_argument('--ip', type=str, default='0.0.0.0', help='IP address to bind the server to')
    parser.add_argument('--port', type=int, default=6665, help='Port to bind the server to')
    parser.add_argument('--workers', type=int, default=None, help='Number of worker threads in the server pool (default: 5 * CPU cores)')
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Logging level')
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper()),
        format='%(asctime)s - %(levelname)s - %(threadName)s - %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create 'files' directory if it doesn't exist at server startup location
    # This ensures FileInterface's os.chdir('files') will succeed.
    if not os.path.exists('files'):
        os.makedirs('files')
        logging.info("Created 'files' directory for server storage.")

    svr = Server(ipaddress=args.ip, port=args.port, max_workers=args.workers)
    svr.start()

    try:
        while svr.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.warning("MainThread (Orchestrator): KeyboardInterrupt. Requesting server stop.")
    finally:
        if svr.is_alive():
            logging.info("MainThread (Orchestrator): Stopping server...")
            svr.stop() 
            svr.join(timeout=10) # Wait for server thread to finish
            if svr.is_alive():
                logging.warning("MainThread (Orchestrator): Server thread did not shut down cleanly after timeout.")
        logging.warning("MainThread (Orchestrator): Application exiting.")

if __name__ == "__main__":
    main()