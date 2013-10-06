#!/usr/bin/env python
import SocketServer, socket, getopt, sys, threading, time, os, \
        copy, pickle, atexit, traceback

# global settings
SOCKET_TIMEOUT = 3
LATENCY_MEASURMENT = 30
LATENCY_TRANSMIT = 60
HEARTBEAT = 5
COORDINATOR_TIMEOUT = 25

# global output file names
LOG_FILE = "overlay.log"
LATENCY_FILE = "latency.log"
PINGS_FILE = "pings.log"
EXCEPTION_FILE = "exceptions.log"

# global variables
is_coordinator = False
member_lock = threading.Lock()
members = dict()
my_port = 13337
pings = dict()
last_ping = 0
last_latency_measurement = 0
last_latency_transmission = 0

# Variables to ensure integrity of the node
# There are 3 threads in the node, if one of them dies, the other will die as well
is_alive = 0

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

def memberlist_update_command(data):
    global members, coordinator
    c = data["coordinator"]
    if c["id"] == coordinator["id"]:
        members = data["members"]
        log_membership()
    # if network is larger than ours, join them instead
    elif len(data["members"]) > len(members):
        members = data["members"]
        if is_coordinator:
            send_new_memberlist()
            is_coordinator = False
        coordinator = c
        # Try to join other network
        seeds = [coordinator]
        connect_to_network()
    return {"command" : "ok"}

def latency_data_command(data):
    member_pings = data["pings"]
    log_pings(data["pings"], data["id"])
    return {"command" : "ok"}

def kicked_out_command(data):
    print "Got kicked out"
    return {"command" : "ok"}

def leave_command(data):
    print(data)
    print(members)
    print(data["id"])
    removeMember(int(data["id"]), "LEAVE")
    return {"command" : "ok"}

# This is the list of network commands which the TCP server accepts. The
# dictionary is of type {string : function}. For example gives command["join"] a
# function pointer to the join_command function.
commands = {"join" : join_command,
            "memberlist_update" : memberlist_update_command,
            "latency_data" : latency_data_command,
            "kicked_out" : kicked_out_command,
            "leave" : leave_command}

# COORDINATOR functions

def addMember(nodeAddress):
    global next_id
    try:
        # Add the new member
        members[next_id] = nodeAddress
        # Log the join event
        log_event(next_id, "JOIN")
        next_id += 1
        # Propagate the new member's list
        send_new_memberlist()
        # Return the id of the new member
        return next_id - 1
    except Exception, e:
        log_exception("EXCEPTION in addMember (failed)", e)
        traceback.print_exc()

#def listMembers():

def removeMember(nodeID, event):
    global members
    if event == "FAIL":
        try:
            # Send kicked_out message to member
            send_node_message(members[nodeID], {"command" : "kicked_out", \
                "coordinator" : coordinator})
        except socket.error, e:
            log_exception("WARNING in removeMember", e)
            traceback.print_exc()
    # Delete member from member's list
    del members[nodeID]
    # Log event
    log_event(nodeID, event)
    # Propagate new member's list
    send_new_memberlist()

def ping(host, port, host_id):
    global my_id
    try:
        # Create a new socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5)
        # Send this nodes id as a UDP message
        data = str(my_id)
        # Start timer
        begin = time.time()
        # Send message to node
        sock.sendto(data.encode(), (host, port+1))
        # Receive reply
        received = sock.recv(1024).decode()
        # Stop timer
        end = time.time()
        # Check that reply was from correct host. Host should reply with its id.
        if str(received) == str(host_id):
            return end-begin
        else:
            return -1
    except Exception, e:
        log_exception("WARNING in ping", e)
        traceback.print_exc()
        return -1

# MEMBER functions

def join(node):
    global coordinator, is_coordinator, members, my_id, my_ip, \
        my_port, last_ping, seeds, next_id
    # This is the join message. It is then processed in the join function of
    # the receiving server.
    bootstrap_message = {"command"  : "join", "address" : 
                         {"ip" : my_ip, "port" : my_port}}
    # Store reply in result variable.
    result = send_message(node["ip"], node["port"], bootstrap_message)
    # This means the node was a member of the overlay network. We then need to
    # send a new message to the coordinator given by the member.
    if result["command"] == "coordinator_info":
        # Set coordinator according to the members reply.
        coordinator = result["coordinator"]
        # Send join request to coordinator.
        result = send_message(coordinator["ip"], coordinator["port"], 
                              bootstrap_message)
        log_status("* Connected to overlay network, id=" +\
                str(result["your_id"]))
    # Store all the information given by the coordinator.
    coordinator = result["coordinator"]
    members = result["members"]
    # This is the ID which every member have. It is given by the coordinator as
    # response to a join request and it is unique for every member. The member
    # with the lowest ID is the oldest member, and the oldest member is always
    # the coordinator.
    my_id = result["your_id"]
    # Set the variable last_ping which keeps track of when we last heard from
    # the coordinator to current time.
    last_ping = time.time()

def leave():
    global coordinator, is_coordinator
    # The coordinator never send a leave request to the network.
    if not is_coordinator:
        #try:
            message = {"command" : "leave", "id" : my_id }
            result = send_message(coordinator["ip"], coordinator["port"], message)
<<<<<<< HEAD
        #except Exception, e:
        #    log_exception("EXCEPTION in leave", e)
=======
        except Exception, e:
            log_exception("EXCEPTION in leave", e)
            traceback.print_exc()
>>>>>>> 8247613b290a9fc656e3f813c06a12e6fcdfb83d

# Heartbeat function (check if all members are alive)
def heartbeat():
    global pings, members, is_coordinator
    # Create a copy of members dict since another thread may change it.
    for nodeID, node in copy.deepcopy(members).items():
        # Dont ping yourself
        if nodeID == my_id:
            continue
        successful = False
        count = 0
        # Try to ping every node two times.
        while not successful and count < 2:
            time = ping(node["ip"], node["port"], nodeID)
            if time > 0:
                pings[nodeID] = time
                print("Pinged node %s in %.2f ms" % (nodeID, pings[nodeID]*1000))
                successful = True
            else:
                count = count + 1
                print("Ping failed")
        # Remove member who doesn't reply to pings.
        if not successful:
            removeMember(nodeID, "FAIL")
    # Propagate new member's list
    send_new_memberlist()
    # Log the current members
    log_membership()

# Measure latency
def measure_latency():
    global pings, members, is_coordinator, coordinator, my_id,\
            last_latency_transmission, LATENCY_TRANSMIT
    try:
        for nodeID, node in copy.deepcopy(members).items():
            if nodeID == my_id:
                continue
            t = ping(node["ip"], node["port"], nodeID)
            if t > 0:
                cal_avg(nodeID, t)
                log_latency(nodeID, t)
                if time.time() > last_latency_transmission + LATENCY_TRANSMIT:
                    message = {"command" : "latency_data",\
                        "pings" : pings, "id" : my_id }
                    send_message(coordinator["ip"], coordinator["port"], message)
                    last_latency_transmission = time.time()
            else:
                log_exception("WARNING in measure_latency", "LATENCY node" +\
                        str(nodeID) + " FAILED")
    except Exception, e:
        log_exception("EXCEPTION in measure_latency", e)
        traceback.print_exc()

def cal_avg(nodeID, new_latency):
    # Calculates a exponential moving average of pings.
    global pings
    a = 0.9
    if not nodeID in pings:
        pings[nodeID] = new_latency
    else:
        new_avg = a*float(pings[nodeID]) + (1-a)*new_latency
        pings[nodeID] = new_avg

def reelect_coordinator():
    global coordinator, is_coordinator, my_ip, my_port, my_id, members, \
    last_ping, next_id
    # Set the oldest member (member with lowest id) to coordinator.
    coord_id = min(members, key=int)
    while len(members):
        # If I am the member with lowest id, I will be the coordinator
        if coord_id == my_id:
            log_status("* Select myself as new coordinator")
            is_coordinator = True
            coordinator = {"ip" : my_ip, "port" : my_port, "id" : my_id}
            next_id = max(members) + 1
            return
        else:
            # Try to ping the new coordinator to see if it is alive. Otherwise
            # delete member from member's list and contact the next member with
            # lowest id.
            try:
                coordinator = members[coord_id]
                coordinator["id"] = coord_id
                #join(coordinator)
                t = ping(coordinator["ip"], coordinator["port"],coord_id)
                if t > 0:
                    log_status("Chose " + str(coord_id) + " as new coordinator")
                    last_ping = time.time()
                    return
            except socket.error, e:
                log_exception("WARINING in reelect_coordinator", e)
                traceback.print_exc()
            del members[coord_id]
            coord_id = min(members, key=int)
    # If all this fails, try to bootstrap again.
    if not len(members):
        connect_to_network()

# HELPER METHODS

def send_message(ip, port, message):
    # Sends a message over tcp sockets and returns the reply
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(pickle.dumps(message))
    return pickle.loads(s.recv(1024))

def send_node_message(node, message):
    # Here node is a dict of type {"ip" : "x.x.x.x", "port" : 1234} 
    send_message(node["ip"], node["port"], message)

def send_new_memberlist():
    # Broadcast the member's list
    global members, my_id
    message = {"command" : "memberlist_update",
               "members" : members,
               "coordinator" : coordinator}
    for nodeID, node in members.items():
        # Dont send to yourself
        if (nodeID == my_id):
            continue
        try:
            send_message(node["ip"], node["port"], message)
        except Exception, e:
            log_exception("WARNING in send_new_memberlist", e)
            traceback.print_exc()

def connect_to_network():
    global coordinator, is_coordinator, my_id, my_ip, \
        my_port, seeds, next_id, members
    # try twice to access the network
    for i in (1, 2):
        for seed in seeds:
            try:
                join(seed)
                return
            except socket.error, e:
                log_exception("INFO in connect_to_network", e)
                traceback.print_exc()
    # Accessing the network failed, then I will be the coordinator
    is_coordinator = True
    my_id = 0
    next_id = 1
    coordinator = {"ip" : my_ip, "port" : my_port, "id" : 0}
    members = {}
    members[my_id] = {"ip" : my_ip, "port" : my_port}
    log_status("* I am now coordinator")


# TCP serversocket, answers to messages coordinating the overlay

class MyTCPServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True
    def server_bind(self):
        # I may reuse the local port. This is to not have to change the port
        # after a unclean exit.
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Then call inherited bind method
        self.socket.bind(self.server_address)

class MyTCPServerHandler(SocketServer.BaseRequestHandler):
    def handle(self): 
        global is_alive, COORDINATOR_TIMEOUT
        if time.time() > is_alive + COORDINATOR_TIMEOUT:
            # too long not alive, kill myself
            log_exception("DEAD in MyTCPServerHandler.handle", "Assume main" + \
                    "thread is dead, kill myself.")
            sys_exit()
        if True:
        #try:
            # Deserialize received data
            data = pickle.loads(self.request.recv(1024).strip())
            # Python magic. commands[data["command]] gives function pointer to
            # one of the server command functions and it is then called with
            # (data) as parameter. Ex. join_command(data)
            reply = commands[data["command"]](data)
            # Send back the reply generated by the commands function
            self.request.sendall(pickle.dumps(reply))
<<<<<<< HEAD
        #except Exception, e:
        #    log_exception("EXCEPTION in MyTCPServerHandler.handle", e)
=======
        except Exception, e:
            log_exception("EXCEPTION in MyTCPServerHandler.handle", e)
            traceback.print_exc()
>>>>>>> 8247613b290a9fc656e3f813c06a12e6fcdfb83d

# UDP serversocket, answers to ping requests

class MyUDPServer(SocketServer.ThreadingUDPServer):
    allow_reuse_address = True

class MyUDPServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        global last_ping, my_port, members, coordinator, my_id, is_alive,\
                COORDINATOR_TIMEOUT
        if time.time() > is_alive + COORDINATOR_TIMEOUT:
            # too long not alive, kill myself
            log_exception("DEAD in MyUDPServerHandler.handle", "Assume main" + \
                    "thread is dead, kill myself.")
            sys_exit()
        try:
            data = self.request[0].decode().strip()
            socket = self.request[1]
            # Reply with our ID
            socket.sendto(str(my_id).encode(), self.client_address)
            # If ping was from coordinator, update last_time variable with the
            # current time
            if str(data) == str(coordinator["id"]):
                last_ping = time.time()
        except Exception, e:
            log_exception("EXCEPTION in MyUDPServerHandler.handle", e)
            traceback.print_exc()

# Log functions

def log_status(msg):
    global LOG_FILE, EXCEPTION_FILE
    for filename in (LOG_FILE, EXCEPTION_FILE):
        f = open(filename, "a")
        f.write(msg + "\n")
        f.close()
    print(msg)

def log_membership():
    global is_coordinator, LOG_FILE
    filename = LOG_FILE
    log_timestamp(filename)
    if not is_coordinator:
        log_coordinator(filename)
    log_members(filename)

def log_event(nodeID, event):
    global LOG_FILE
    filename = LOG_FILE
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
    # Make copy of members since another thread may change it 
    for nodeID, node in copy.deepcopy(members).items():
        if not is_empty:
            msg = msg + ", "
        msg = msg + "node" + str(nodeID)
        is_empty = False
    msg = msg + "]"
    f.write(msg + "\n ")
    print(msg)
    f.close()
    log_exception("INFO", "My id: node" + str(my_id))

def log_latency(nodeID, new_latency):
    global pings, my_id, LATENCY_FILE
    f = open(LATENCY_FILE, "a")
    msg = "["+str(my_id)+", "+str(nodeID)+", "+str(new_latency)+\
            ", "+str(pings[nodeID])+", "+str(time.time())+"]"
    f.write(msg+"\n")
    f.close()
    print(msg)

def log_pings(ping_list, sourceID):
    global PINGS_FILE
    f = open(PINGS_FILE, "a")
    print("LOG PINGS: ")
    for destID, line in ping_list.items():
        msg = "[" + str(sourceID) + ", " + str(destID) + ", " + \
                str(line) + "]"
        f.write(msg + "\n")
        print(msg)
    f.close()

def log_exception(info, exception):
    global EXCEPTION_FILE
    f = open(EXCEPTION_FILE, "a")
    msg = str(time.time()) + ": " + info + "\n"
    msg = msg + "    " + str(exception)
    print(msg)
    f.write(msg + "\n")
    f.close()

def before_exit():
    global pingServer, tcpServer, is_alive
    # Shutdown servers and exit
    # --- Commented out since it don't work with python 2.5
    # pingServer.shutdown()
    # tcpServer.shutdown()
    leave()
    is_alive = -1000
    sys.exit(0)

# Main Thread
def main_thread_body():
    global last_latency_measurement, LATENCY_MEASURMENT, COORDINATOR_TIMEOUT,\
            is_alive, is_coordinator, last_ping
    try:
        while (True):
            is_alive = time.time()
            try:
                # Measure latency
                if time.time() > last_latency_measurement + LATENCY_MEASURMENT:
                    measure_latency()
                    last_latency_measurement = time.time()
                # Coordinator functions
                if is_coordinator:
                    heartbeat()
                    time.sleep(HEARTBEAT)
                # Member functions
                if not is_coordinator:
                    if time.time() > last_ping + COORDINATOR_TIMEOUT:
                        log_exception("WARNING in main", "Coordinator has died")
                        try:
                            join(coordinator)
                        except socket.error, e:
                            log_exception("EXCEPTION in main_thread_body, member", e)
                            reelect_coordinator()
                            heartbeat()
                # Wait 1 second and then rerun the loop
                time.sleep(1)
            except Exception, e:
                log_exception("EXCEPTION in main_thread_body", e)
                traceback.print_exc()
    except (KeyboardInterrupt, Exception), e:
        # Print the unexpected exception and exit
        print e
        traceback.print_exc()
        before_exit()

def read_nodes_from_file():
    # Load the seeds file
    global seeds
    f = open("nodes.txt", "r")
    seeds = []
    for line in f:
        s = line.split(":")
        seeds.append({"ip" : s[0], "port" : int(s[1].strip())})


# main function, initialize overlay node                

def main(argv):
    global my_ip, my_port, seeds, last_ping, last_latency_measurement,\
            SOCKET_TIMEOUT, HEARTBEAT, LATENCY_MEASURMENT, \
            LATENCY_TRANSMIT, COORDINATOR_TIMEOUT, \
            pingServer, tcpServer, is_alive
    # Register before_exit() as the function which does cleanup before python
    # exits the program
    atexit.register(before_exit)
    # Set timeouts for out sockets
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    # Now follows code which has to do with command line parameters
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
    # End of command line parameters code
    is_alive = time.time()
    # delete previous log files
    log_status("* Start-up node")
    # get ip address 
    my_ip = socket.gethostbyname(socket.gethostname())
    log_exception("INFO in main", "Binding TCP to " + my_ip + ":" +\
            str(my_port))
    log_exception("INFO in main", "Binding UDP to " + my_ip + ":" +\
            str(my_port+1))
    # listen for UDP messages for ping request
    pingServer = MyUDPServer(('0.0.0.0', my_port+1), MyUDPServerHandler)
    pingThread = threading.Thread(target=pingServer.serve_forever)
    pingThread.daemon = True
    pingThread.start()
    # read seeds (list of other possible nodes in the overlay network)
    #f = open("seeds.txt", "r")
    #seeds = pickle.loads(f.read())
    #print(seeds)
    #f.close()
    read_nodes_from_file()
    # connect to the overlay and listen for TCP messages (overlay communication messages)
    connect_to_network()
    tcpServer = MyTCPServer(('0.0.0.0', my_port), MyTCPServerHandler)
    tcpThread = threading.Thread(target=tcpServer.serve_forever)
    tcpThread.daemon = True
    tcpThread.start()
    main_thread_body()

if __name__ == "__main__":
    main(sys.argv[1:])
