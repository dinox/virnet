#!/usr/bin/env python3
import socketserver, socket, getopt, sys, threading, time, os, \
        copy
        
is_client = False
host = '0.0.0.0'
port = 8098

class MyUDPServer(socketserver.ThreadingUDPServer):
    allow_reuse_address = True

class MyUDPServerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        while (True):
            try:
                data = self.request[0].decode()
                socket = self.request[1]
                print("{0} wrote".format(self.client_address[0]))	
                print(data)
                socket.sendto(data.encode(), self.client_address)
            except Exception as e:
                print("Exception while receiving message: ", e)
            
def ping_udp(host, port):
    try:
        data = "ping"
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(data.encode(), (host, port))
        received = sock.recv(1024).decode()
        return received == data
    except Exception as e:
        print("Exception while sending ping: ", e)
        return False
            
def main(argv):
    global is_client, host, port
    socket.setdefaulttimeout(2)
    try:
        opts, args = getopt.getopt(argv,"",["client"])
    except getopt.GetoptError:
        print('ping.py [--client]')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '--client':
            is_client = True
        else:
            print('ping.py [--client]')
            sys.exit(2)
    if is_client:
        print("Start client...")
        if ping_udp(host, port):
            print("Ping successful")
        else:
            print("Ping failed")
    else:
        print("Start server...")
        server = MyUDPServer((host, port), MyUDPServerHandler)
        threading.Thread(target=server.serve_forever).start()

if __name__ == "__main__":
    main(sys.argv[1:])
