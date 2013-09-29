#!/usr/bin/env python3
import socketserver, socket, json, getopt, sys, threading, time, os, \
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
    last_ping = time.clock()
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
        if (nodeID == my_id):
            continue
        ping_message = {"command" : "ping", "payload" : ""}
        try:
            begin = time.clock()
            print(send_message(node["ip"], node["port"], ping_message))
            end = time.clock()
            pings[nodeID] = end-begin
            print("Pinged node %s in %.2f ms" % (nodeID, pings[nodeID]*1000))
        except ConnectionRefusedError:
            del members[nodeID]
    send_new_memberlist()
    member_lock.release()

# HELPER METHODS

def send_message(ip, port, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(bytes(json.dumps(message), 'UTF-8'))
    return json.loads(s.recv(1024).decode('UTF-8'))

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
            last_ping = time.clock()
            return
        except ConnectionRefusedError:
            print("could not connect to %s:%s " % (seed["ip"], seed["port"]))
            pass
    is_coordinator = True
    my_id = 0
    coordinator = {"ip" : my_ip, "port" : my_port}
    members = {0:coordinator}
    print("I am now coordinator")

class MyTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

class MyTCPServerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            data = json.loads(self.request.recv(1024).decode('UTF-8').strip())
            print(data)
            reply = commands[data["command"]](data)
            self.request.sendall(bytes(json.dumps(reply), 'UTF-8'))
        except Exception as e:
            print("Exception wile receiving message: ", e)

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
    f = open("seeds.txt", "r")
    seeds = json.loads(f.read())
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
                if time.clock() > last_ping + 25/3600.0:
                    print("coordinator has died!")
                    os._exit(0)
                time.sleep(1)
    except KeyboardInterrupt:
        os._exit(0)

if __name__ == "__main__":
    main(sys.argv[1:])
