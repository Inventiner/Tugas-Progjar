from socket import *
import socket
import threading
import logging
import time
import sys
import argparse
import os
from concurrent.futures import ProcessPoolExecutor

from file_protocol import FileProtocol

def process_client_connection(connection, address):
    fp_instance = FileProtocol()
    client_handler = ProcessTheClient(connection, address, fp_instance)
    client_handler.run()

class ProcessTheClient():
    def __init__(self, connection, address, fp_protocol_instance):
        self.connection = connection
        self.address = address
        self.fp_protocol = fp_protocol_instance

    def run(self):
        buffer = ""
        try:
            self.connection.settimeout(120)
            while True:
                data = self.connection.recv(16384)
                if not data: 
                    logging.info(f"Connection closed by {self.address}")
                    break
                try:
                    buffer += data.decode()
                except UnicodeDecodeError as e:
                    logging.error(f"UnicodeDecodeError from {self.address}: {e}. Buffer: {buffer[:100]}...")
                    break 
                
                while "\r\n\r\n" in buffer:
                    command_to_process, buffer = buffer.split("\r\n\r\n", 1)
                    
                    hasil_json_str = self.fp_protocol.proses_string(command_to_process)
                    
                    response_to_send = hasil_json_str + "\r\n\r\n"
                    self.connection.sendall(response_to_send.encode())

        except socket.timeout:
            logging.warning(f"Socket timeout for client {self.address}.")
        except ConnectionResetError:
            logging.warning(f"Connection reset by client {self.address}.")
        except BrokenPipeError:
            logging.warning(f"Broken pipe with client {self.address} (client likely closed connection abruptly).")
        except Exception as e:
            logging.error(f"Unexpected error processing client {self.address}: {e}", exc_info=True)
        finally:
            self.connection.close()
            logging.info(f"Connection with {self.address} ended.")


class Server(threading.Thread):
    def __init__(self,ipaddress='0.0.0.0',port=6665, max_workers=None):
        super().__init__()
        self.ipinfo=(ipaddress,port)
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        if max_workers is None or max_workers <= 0:
            self.max_workers = os.cpu_count() or 1
            logging.info(f"Max workers not specified or invalid, defaulting to CPU count: {self.max_workers}")
        else:
            self.max_workers = max_workers
            
        self.shutdown_event = threading.Event()
        self.executor = None
    
    def run(self):
        logging.warning(f"MPPool Server starting on {self.ipinfo}, max worker processes: {self.max_workers}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(128)
        self.my_socket.settimeout(1.0)

        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                self.executor = executor
                logging.info(f"ProcessPoolExecutor started with {self.max_workers} worker processes.")

                while not self.shutdown_event.is_set():
                    try:
                        connection, client_address = self.my_socket.accept()
                        logging.info(f"MainProc: Accepted connection from {client_address}")
                        self.executor.submit(process_client_connection, connection, client_address)
                    except socket.timeout:
                        continue
                    except OSError as e:
                        if self.shutdown_event.is_set():
                            logging.info("MP Server: Socket operation interrupted by shutdown.")
                            break
                        logging.error(f"MP Server: Socket error during accept: {e}", exc_info=True)
                        break 
        except KeyboardInterrupt:
            logging.warning("MP Server: KeyboardInterrupt received, initiating shutdown...")
        except Exception as e:
            logging.error(f"MP Server: Main loop encountered an unhandled error: {e}", exc_info=True)
        finally:
            self.shutdown_event.set()
            logging.warning("MP Server: Shutdown initiated (or already in progress from with-block exit).")
            
            if self.my_socket:
                try:
                    self.my_socket.close()
                    logging.info("MP Server: Listening socket closed.")
                except Exception as e:
                    logging.error(f"MP Server: Error closing listening socket: {e}")
            
            logging.warning("MP Server: Run method finishing.")

    def stop(self):
        logging.info("MP Server: Stop requested.")
        self.shutdown_event.set()

def main():
    parser = argparse.ArgumentParser(description="File Server with ProcessPoolExecutor")
    parser.add_argument('--ip', type=str, default='0.0.0.0', help='IP address to bind the server to')
    parser.add_argument('--port', type=int, default=6665, help='Port to bind the server to')
    parser.add_argument('--workers', type=int, default=None, help='Number of worker processes (default: CPU cores)')
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Logging level')
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper()),
        format='%(asctime)s - %(levelname)s - %(processName)s (%(process)d) - %(threadName)s - %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    if not os.path.exists('files'):
        os.makedirs('files')
        logging.info("Created 'files' directory for server storage.")

    svr = Server(ipaddress=args.ip, port=args.port, max_workers=args.workers)
    svr.start()

    try:
        while svr.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.warning("MainProc (Orchestrator): KeyboardInterrupt. Requesting server stop.")
    finally:
        if svr.is_alive():
            logging.info("MainProc (Orchestrator): Stopping server...")
            svr.stop() 
            svr.join(timeout=10)
            if svr.is_alive():
                logging.warning("MainProc (Orchestrator): Server thread did not shut down cleanly after timeout.")
        logging.warning("MainProc (Orchestrator): Application exiting.")

if __name__ == "__main__":
    main()