#!/usr/bin/env python2
import SocketServer, socket, json, getopt, sys, threading, time, os, \
        copy

is_coordinator = False
member_lock = threading.Lock()
my_ip = '127.0.0.1'
my_port = 13337
pings = dict()
last_ping = 0

# COMMANDS

def bootstrap(data):
    if not is_coordinator:
        return coordinator_info(data)
    nodeID = addMember(data["address"]) 
    return {"command"   : "hello", 
            "your_id"   : nodeID,
            "members"   : members}

def coordinator_info(data):
    return {"command"       : "coordinator_info",
            "coordinator"   : coordinator}
def list_members(data):
    return {"command" : "reply",
            "members" : members}
def ping(data):
    global last_ping
    last_ping = time.time()
    return {"command"   : "ping_reply",
            "payload"   : data["payload"]}

def memberlist_update(data):
    members = data["members"]
    return {"command" : "ok"}

commands = {"bootstrap" : bootstrap,
            "ping"      : ping,
            "memberlist_update" : memberlist_update}

# PROBLEM SPECIFIC METHODS

def addMember(nodeAddress):
    member_lock.acquire()
    nbr_of_members = len(members)
    members[nbr_of_members] = nodeAddress
    member_lock.release()
    return nbr_of_members

def heartbeat():
    global pings, members
    member_lock.acquire()
    for nodeID, node in copy.deepcopy(members).items():
        if nodeID == my_id:
            continue
        ping_message = {"command" : "ping", "payload" : ""}
        successful = False
        count = 0
        while not successful and count < 3:
            print("Start ping {0}, {1}, {2}", node["ip"], node["port"], nodeID)
            time = ping_udp(node["ip"], node["port"], nodeID)
            if time >= 0:
                pings[nodeID] = time
                print("Pinged node %s in %.2f ms" % (nodeID, pings[nodeID]*1000))
                successful = True
            else:
                count = count + 1
                print("Ping failed")
        if not successful:
            del members[nodeID]
    send_new_memberlist()
    member_lock.release()

# HELPER METHODS

def send_message(ip, port, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(json.dumps(message))
    return json.loads(s.recv(1024))

def send_new_memberlist():
    global members, my_id
    message = {"command" : "memberlist_update",
               "members" : members}
    print(members)
    for nodeID, node in members.items():
        if (nodeID == my_id):
            continue
        send_message(node["ip"], node["port"], message)



def connect_to_network():
    global coordinator, is_coordinator, members, my_id, my_ip, \
        my_port, seeds, last_ping
    bootstrap_message = {"command"  : "bootstrap", "address" : 
                         {"ip" : my_ip, "port" : my_port}}
    for seed in seeds:
        try:
            result = send_message(seed["ip"], seed["port"], bootstrap_message)
            print(result)
            if result["command"] == "coordinator_info":
                coordinator = result["coordinator"]
                result = send_message(coordinator["ip"], coordinator["port"], 
                                      bootstrap_message)
                print("connected to overlay network")
                print(result)
            coordinator = seed
            members = result["members"]
            my_id = result["your_id"]
            last_ping = time.time()
            return
        except socket.error:
            print("could not connect to %s:%s " % (seed["ip"], seed["port"]))
            pass
    is_coordinator = True
    my_id = 0
    coordinator = {"ip" : my_ip, "port" : my_port}
    members = {0:coordinator}
    print("I am now coordinator")
    
# Logging helper functions

def log_members_client():
    f = open("overlay.log", "a")


# TCP serversocket, answers to messages coordinating the overlay

class MyTCPServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True

class MyTCPServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
	    data = json.loads(self.request.recv(1024).strip())
            print(data)
            reply = commands[data["command"]](data)
            self.request.sendall(json.dumps(reply))
	except Exception:
            print("Exception wile receiving message: ")
            
# UDP serversocket, answers to ping requests

class MyUDPServer(SocketServer.ThreadingUDPServer):
    allow_reuse_address = False

class MyUDPServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        global last_ping
        try:
            data = self.request[0].strip()
            socket = self.request[1]
            print("Received ping from {0}.".format(self.client_address[0]))
            socket.sendto(data, self.client_address)
            #TODO: check if sender is the coordinator
            last_ping = time.time()
        except Exception:
            print("Exception while receiving message: ")

# UDP ping request
                
def ping_udp(host, port, host_id):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        data = str(host_id)
        begin = time.time()
        sock.sendto(data.encode(), (host, port+1))
        received = sock.recv(1024).decode()
        end = time.time()
        if received == data:
            return end-begin
        else:
            return -1
    except Exception:
        print("Exception while sending ping: ")
        return -1                
                
# main function, initialize overlay node                

def main(argv):
    global my_ip, my_port, seeds, last_ping
    socket.setdefaulttimeout(2)
    try:
        opts, args = getopt.getopt(argv,"hi:p:",["ip=", "port="])
    except getopt.GetoptError:
        print('overlay.py --ip <node ip>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('overlay.py -ip <node ip>')
            sys.exit()
        elif opt in ("-i", "--ip"):
            my_ip = arg
        elif opt in ("-p", "--port"):
            my_port = int(arg)
        else:
            print('overlay.py -ip <node ip>')
            sys.exit(2)
    # listen for UPD messages for ping request
    pingServer = MyUDPServer(('0.0.0.0', my_port+1), MyUDPServerHandler)
    threading.Thread(target=pingServer.serve_forever).start()
    # read seeds (list of other possible nodes in the overlay network)
    f = open("seeds.txt", "r")
    seeds = json.loads(f.read())
    # connect to the overlay and listen for TCP messages (overlay communication messages)
    connect_to_network()
    server = MyTCPServer(('0.0.0.0', my_port), MyTCPServerHandler)
    threading.Thread(target=server.serve_forever).start()
    try:
        while (True):
            if is_coordinator:
                print("Heartbeat")
                heartbeat()
                time.sleep(10)
            if not is_coordinator:
                if time.time() > last_ping + 25:
                    print("coordinator has died!")
                    os._exit(0)
                time.sleep(1)
    except KeyboardInterrupt:
        os._exit(0)

if __name__ == "__main__":
    main(sys.argv[1:])
