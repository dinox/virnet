#!/usr/bin/env python
import SocketServer, socket, getopt, sys, threading, time, os, \
        copy, pickle

is_coordinator = False
member_lock = threading.Lock()
members = dict()
my_port = 13337
pings = dict()
last_ping = 0
last_latency_measurement = 0

# COMMANDS

def join_command(data):
    if not is_coordinator:
        return coordinator_info_command(data)
    nodeID = addMember(data["address"]) 
    return {"command"   : "hello", 
            "your_id"   : nodeID,
            "coordinator" : coordinator,
            "members"   : members}

def coordinator_info_command(data):
    return {"command"       : "coordinator_info",
            "coordinator"   : coordinator}
def list_members_command(data):
    return {"command" : "reply",
            "members" : members}
def ping_command(data):
    global last_ping
    last_ping = time.time()
    return {"command"   : "ping_reply",
            "payload"   : data["payload"]}

def memberlist_update_command(data):
    global members
    members = data["members"]
    log_membership()
    return {"command" : "ok"}

def latency_data_command(data):
    member_pings = data["pings"]
    log_pings(data["pings"], data["id"])
    print("latency_data_command received: ")
    print(member_pings)
    return {"command" : "ok"}

def kicked_out_command(data):
    print "Got kicked out"
    return {"command" : "ok"}

commands = {"join" : join_command,
            "ping"      : ping_command,
            "memberlist_update" : memberlist_update_command,
            "latency_data" : latency_data_command,
            "kicked_out" : kicked_out_command}

# COORDINATOR functions

def addMember(nodeAddress):
    global next_id
    members[next_id] = nodeAddress
    log_event(next_id, "JOIN")
    next_id += 1
    return next_id - 1
#def listMembers():

def removeMember(nodeID, event):
    global members
    try:
        send_node_message(members[nodeID], {"command" : "kicked_out", \
            "coordinator" : coordinator})
    except socket.error, e:
        print e
    del members[nodeID]
    log_event(nodeID, event)
    send_new_memberlist()

def ping(host, port, host_id):
    global my_id
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5)
        data = str(my_id)
        begin = time.time()
        sock.sendto(data.encode(), (host, port+1))
        received = sock.recv(1024).decode()
        end = time.time()
        if str(received) == str(host_id):
            return end-begin
        else:
            return -1
    #TODO: catch timeout exception, don't print!
    except Exception, e:
        print("Exception while sending ping: %s" % e)
        return -1

# MEMBER functions

def join():
    pass

def leave():
    pass

# Heartbeat function (check if all members are alive)
def heartbeat():
    global pings, members, is_coordinator
    for nodeID, node in copy.deepcopy(members).items():
        if nodeID == my_id:
            continue
        successful = False
        count = 0
        while not successful and count < 3:
            print("Start ping {0}, {1}, {2}", node["ip"], node["port"], nodeID)
            time = ping(node["ip"], node["port"], nodeID)
            if time >= 0:
                pings[nodeID] = time
                print("Pinged node %s in %.2f ms" % (nodeID, pings[nodeID]*1000))
                successful = True
            else:
                count = count + 1
                print("Ping failed")
        if not successful:
            removeMember(nodeID, "FAIL")
    send_new_memberlist()
    log_membership()

# Measure latency
def measure_latency():
    global pings, members, is_coordinator, coordinator, my_id
    try:
        for nodeID, node in copy.deepcopy(members).items():
            if nodeID == my_id:
                continue
            time = ping(node["ip"], node["port"], nodeID)
            if time > 0:
                cal_avg(nodeID, time)
                log_latency(nodeID, time)
                message = {"command" : "latency_data",\
                        "pings" : pings, "id" : my_id }
                send_message(coordinator["ip"], coordinator["port"], message)
            else:
                print("ping in measure_latency failed")
    except Exception, e:
        print("exception in measure_latency %s" % e)

def cal_avg(nodeID, new_latency):
    global pings
    a = 0.9
    if not nodeID in pings:
        pings[nodeID] = 1
    new_avg = a*float(pings[nodeID]) + (1-a)*new_latency
    pings[nodeID] = new_avg

def reelect_coordinator():
    global coordinator, is_coordinator, my_ip, my_port, my_id, members, \
    last_ping, next_id
    print "should now reelect a coordinator"
    coord_id = min(members, key=int)
    while len(members):
        if coord_id == my_id:
            is_coordinator = True
            coordinator = {"ip" : my_ip, "port" : my_port, "id" : 0}
            next_id = max(members) + 1
            del members[my_id]
            return
        else:
            time.sleep(10)
            try:
                coordinator = members[coord_id]
                coordinator["id"] = coord_id
                join(coordinator)
                print "Chose %i as new coordinator" % coord_id
                return
            except socket.error:
                print("could not connect to %s:%s " % (coordinator["ip"], \
                    coordinator["port"]))
            del members[coord_id]
            coord_id = members[min(members)]
    if not len(members):
        connect_to_network()

# HELPER METHODS

def send_message(ip, port, message):
    print message
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(pickle.dumps(message))
    return pickle.loads(s.recv(1024))

def send_node_message(node, message):
    send_message(node["ip"], node["port"], message)
def send_new_memberlist():
    global members, my_id
    message = {"command" : "memberlist_update",
               "members" : members,
               "coordinator" : coordinator}
    print(members)
    for nodeID, node in members.items():
        if (nodeID == my_id):
            continue
        try:
            send_message(node["ip"], node["port"], message)
        except Exception, e:
            print "Exception occured when sending memberlist to node%d (%s,%d)"\
                    % (nodeID, node["ip"], node["port"])
            print e

def join(node):
    global coordinator, is_coordinator, members, my_id, my_ip, \
        my_port, last_ping, seeds, next_id
    bootstrap_message = {"command"  : "join", "address" : 
                         {"ip" : my_ip, "port" : my_port}}
    result = send_message(node["ip"], node["port"], bootstrap_message)
    print(result)
    if result["command"] == "coordinator_info":
        coordinator = result["coordinator"]
        result = send_message(coordinator["ip"], coordinator["port"], 
                              bootstrap_message)
        print("connected to overlay network")
        print(result)
    coordinator = result["coordinator"]
    members = result["members"]
    my_id = result["your_id"]
    last_ping = time.time()

def connect_to_network():
    global coordinator, is_coordinator, my_id, my_ip, \
        my_port, seeds, next_id, members
    for seed in seeds:
        try:
            join(seed)
            return
        except socket.error:
            print("could not connect to %s:%s " % (seed["ip"], seed["port"]))
            pass
    is_coordinator = True
    my_id = 0
    next_id = 1
    coordinator = {"ip" : my_ip, "port" : my_port, "id" : 0}
    members = {}
    print("I am now coordinator")
    

# TCP serversocket, answers to messages coordinating the overlay

class MyTCPServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

class MyTCPServerHandler(SocketServer.BaseRequestHandler):
   def handle(self):
        try:
            data = pickle.loads(self.request.recv(1024).strip())
            print(data)
            reply = commands[data["command"]](data)
            self.request.sendall(pickle.dumps(reply))
        except Exception, e:
            print ("Exception while receiving message: %s" % e)
            
# UDP serversocket, answers to ping requests

class MyUDPServer(SocketServer.ThreadingUDPServer):
    allow_reuse_address = True

class MyUDPServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        global last_ping, my_port, members, coordinator
        try:
            data = self.request[0].decode().strip()
            socket = self.request[1]
            socket.sendto(str(my_id).encode(), self.client_address)
            if str(data) == str(coordinator["id"]):
                last_ping = time.time()
        except Exception, e:
            print("Exception while receiving message: %s" % e)

# Log functions

def initialize_log_files():
    for filename in ("overlay.log", "latency.log"):
        f = open(filename, "a")
        f.write("*** Start node ***\n")
        f.close()

def log_membership():
    global is_coordinator
    filename = "overlay.log"
    log_timestamp(filename)
    if not is_coordinator:
        log_coordinator(filename)
    log_members(filename)

def log_event(nodeID, event):
    filename = "overlay.log"
    log_timestamp(filename)
    tab = "    "
    msg = tab + "[EVENT " + event + "]: node" + str(nodeID)
    f = open(filename, "a")
    f.write(msg + "\n")
    print(msg)
    f.close()
    log_members(filename)

def log_timestamp(filename):
    f = open(filename, "a")
    msg = str(time.time()) + ":"
    f.write(msg + "\n")
    print(msg)
    f.close()

def log_coordinator(filename):
    global coordinator
    f = open(filename, "a")
    tab = "    "
    msg = tab + "[COORDINATOR]: node" + str(coordinator["id"])
    f.write(msg + "\n")
    print(msg)
    f.close()

def log_members(filename):
    global is_coordinator, members
    f = open(filename, "a")
    tab = "    "
    msg = tab + "[MEMBERS]: ["
    is_empty = True
    for nodeID, node in copy.deepcopy(members).items():
        if not is_empty:
            msg = msg + ", "
        msg = msg + "node" + str(nodeID)
        is_empty = False
    msg = msg + "]"
    f.write(msg + "\n ")
    print(msg)
    f.close()

def log_latency(nodeID, new_latency):
    global pings, my_id
    f = open("latency.log", "a")
    msg = "["+str(my_id)+", "+str(nodeID)+", "+str(new_latency)+\
            ", "+str(pings[nodeID])+", "+str(time.time())+"]"
    f.write(msg+"\n")
    f.close()
    print(msg)

def log_pings(ping_list, sourceID):
    f = open("pings.log", "a")
    print("LOG PINGS: ")
    for destID, line in ping_list.items():
        msg = "[" + str(sourceID) + ", " + str(destID) + ", " + \
                str(line) + "]"
        f.write(msg + "\n")
        print(msg)
    f.close()

# main function, initialize overlay node                

def main(argv):
    global my_ip, my_port, seeds, last_ping, last_latency_measurement
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
    # delete previous log files
    initialize_log_files()
    my_ip = socket.gethostbyname(socket.gethostname())
    # get ip address 
    print "Binding TCP to %s:%s" % (my_ip, str(my_port)) 
    print "Binding UDP to %s:%s" % (my_ip, str(my_port+1)) 
    # listen for UPD messages for ping request
    pingServer = MyUDPServer(('0.0.0.0', my_port+1), MyUDPServerHandler)
    threading.Thread(target=pingServer.serve_forever).start()
    # read seeds (list of other possible nodes in the overlay network)
    f = open("seeds.txt", "r")
    seeds = pickle.loads(f.read())
    f.close()
    # connect to the overlay and listen for TCP messages (overlay communication messages)
    connect_to_network()
    server = MyTCPServer(('0.0.0.0', my_port), MyTCPServerHandler)
    threading.Thread(target=server.serve_forever).start()
    try:
        while (True):
            if time.time() > last_latency_measurement + 3:
                measure_latency()
                last_latency_measurement = time.time()
            if is_coordinator:
                print("Heartbeat")
                heartbeat()
                time.sleep(5)
            if not is_coordinator:
                if time.time() > last_ping + 25:
                    print("coordinator has died!")
                    try:
                        join(coordinator)
                    except socket.error, e:
                        print e
                        reelect_coordinator()
                time.sleep(1)
    except (KeyboardInterrupt, Exception), e:
        print e
# Shutdown servers and exit
        server.shutdown()
        pingServer.shutdown()
        leave()
        os._exit(0)
if __name__ == "__main__":
    main(sys.argv[1:])
