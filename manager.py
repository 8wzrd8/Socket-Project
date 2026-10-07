import socket
import sys
import random



def register(peers, name, ip, mport, pport):

    #must be alphabetic 
    if not name.isalpha() or len(name) > 15:
        return "FAILURE"

    #duplicate r
    if name in peers:
        return "FAILURE"

    #need to check which ports are alr in use
    #save them in here
    if mport == pport:
        return "FAILURE"

    used = set()
    for p in peers.values():
        used.add((p["ip"], p["mport"]))
        used.add((p["ip"], p["pport"]))

    if (ip, mport) in used or (ip, pport) in used:
        return "FAILURE"

    #we made it to here so we are fine to store the peer
    peers[name] = {"ip": ip, "mport": mport, "pport": pport, "state": "Free"}
    return "SUCCESS"


def setup(peers, dht, name, n, year):

    #requester is not registered / not free, n < 3, or a DHT already exists
    if name not in peers or peers[name]["state"] != "Free" or n < 3 or dht["exists"]:
        return "FAILURE"

    #collect all OTHER free peers
    free = []
    for p in peers:
        if p != name and peers[p]["state"] == "Free":
            free.append(p)

    #need at least n-1 
    if len(free) < (n - 1):
        return "FAILURE"

    subset = random.sample(free, n - 1)

    #requester becomes leader everyone else selected becomes InDHT
    peers[name]["state"] = "Leader"
    for p in subset:
        peers[p]["state"] = "InDHT"

    members = [name] + subset
    dht.update(exists=True, building=True, leader=name, members=members)

    #"name,ip,p-port"
    parts = []
    for m in members:
        parts.append(f"{m},{peers[m]['ip']},{peers[m]['pport']}")
    tuple_str = ";".join(parts)

    return f"SUCCESS|{tuple_str}"


def complete(dht, name):
     #has to be sent by the leader to approve or mid build 
    if not dht["exists"] or not dht["building"] or name != dht["leader"]:
        return "FAILURE"

    dht["building"] = False
    return "SUCCESS"


def query(peers, dht, name):
    # Failure conditions:
    # 1. DHT not complete
    # 2. name is not registered a peer
    # 3. peer is within InDHT
    querysuccess = False
    peerfound = False
    # Condition 1
    if not dht["exists"]:
        # dht["building"] is checked in the main loop
        print("DHT does not exist")
        return "FAILURE"
    for index in peers:
        # Condition 2
        if index == name:
            # Condition 3
            peerfound = True
            if peers[index]["state"] == "Free":
                querysuccess = True
    if querysuccess:
        # Send 3-tuple of peer name, peer IPv4, peer pport
        # Format of message is SUCCESS|<peer name>,<peer IPv4>,<peer pport>
        randompeer = (random.sample(dht["members"], 1))[0]
        print(f"Successfully sent information of [peer {randompeer}]")
        # Use peer list
        return f"SUCCESS|{randompeer},{peers[randompeer]['ip']},{peers[randompeer]['pport']}"
    if peerfound:
        print("Peer is not Free")
    else:
        print("Peer is not registered")
    return "FAILURE"

def leave(peers, dht, name):
    #has to be an actual member of the dht that's leaving
    if not dht["exists"] or name not in dht["members"]:
        return "FAILURE"
 
    #stash who's leaving
    dht["rebuilding"] = True
    dht["pending_peer"] = name
    dht["pending_op"] = "leave"

    return "SUCCESS"

def join(peers, dht, name):
    #has to be a free peer asking to join an existing dht
    if not dht["exists"] or name not in peers or peers[name]["state"] != "Free":
        return "FAILURE"
 
    dht["rebuilding"] = True
    dht["pending_peer"] = name
    dht["pending_op"] = "join"
 
    #give the joiner the current leaders tuple so it knows who to contact
    leader = dht["leader"]
    return f"SUCCESS|{leader},{peers[leader]['ip']},{peers[leader]['pport']}"
 
def rebuilt(peers, dht, name, new_leader):
    #has to match whichever peer actually kicked off the leave/join
    if not dht["rebuilding"] or name != dht["pending_peer"]:
        return "FAILURE"
 
    if dht["pending_op"] == "leave":
        dht["members"].remove(name)
        peers[name]["state"] = "Free"
    else:
        dht["members"].append(name)
        peers[name]["state"] = "InDHT"
 
    if new_leader not in dht["members"]:
        return "FAILURE"
 
    #set states accordingly whoevers the new leader gets promoted
    #everyone else surviving in the ring is just InDHT
    for m in dht["members"]:
        peers[m]["state"] = "Leader" if m == new_leader else "InDHT"
 
    dht["leader"] = new_leader
    dht["rebuilding"] = False
    dht["pending_peer"] = None
    dht["pending_op"] = None
 
    return "SUCCESS"
 
 
def deregister(peers, name):
    #cant deregister while actively in the dht
    if name not in peers or peers[name]["state"] == "InDHT":
        return "FAILURE"
 
    del peers[name]

    return "SUCCESS"
 
 
def teardown(dht, name):
    #only the leader can tear down the dht
    if not dht["exists"] or name != dht["leader"]:
        return "FAILURE"
 
    dht["tearing_down"] = True
    return "SUCCESS"
 
 
def teardown_complete(peers, dht, name):
    if not dht["tearing_down"] or name != dht["leader"]:
        return "FAILURE"
 
    #everyone who was in the dht goes back to being free
    for m in dht["members"]:
        peers[m]["state"] = "Free"
 
    dht["exists"] = False
    dht["building"] = False
    dht["rebuilding"] = False
    dht["tearing_down"] = False
    dht["leader"] = None
    dht["members"] = []
    dht["pending_peer"] = None
    dht["pending_op"] = None
 
    return "SUCCESS"

def main():

    if len(sys.argv) != 2:
        sys.exit(1)

    port = int(sys.argv[1])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", port))

    peers = {}
    dht = {
        "exists": False,
        "building": False,
        "rebuilding": False,
        "tearing_down": False,
        "leader": None,
        "members": [],
        "pending_peer": None,
        "pending_op": None,
    }

    print(f"[manager] listening on port {port}")

    #manager loops forever waiting for messages from peers
    while True:
        result = sock.recvfrom(4096)
        data = result[0]
        addr = result[1]

        msg = data.decode().strip()
        print(f"[manager] recv from {addr}: {msg}")

        parts = msg.split("|")
        cmd = parts[0].upper()
        
        if dht["building"] and cmd != "DHT-COMPLETE":
            reply = "FAILURE"

        elif cmd == "REGISTER":
            name = parts[1]
            ip = parts[2]
            mport = int(parts[3])
            pport = int(parts[4])

            reply = register(peers, name, ip, mport, pport)

        elif cmd == "SETUP-DHT":
            name = parts[1]
            n = int(parts[2])
            year = parts[3]
                
            reply = setup(peers, dht, name, n, year)

        elif cmd == "DHT-COMPLETE":
            name = parts[1]

            reply = complete(dht, name)

        elif cmd == "QUERY-DHT":
            name = parts[1]
            
            reply = query(peers, dht, name)
 
        elif cmd == "LEAVE-DHT":
            name = parts[1]

            reply = leave(peers, dht, name)
 
        elif cmd == "JOIN-DHT":
            name = parts[1]

            reply = join(peers, dht, name)
 
        elif cmd == "DHT-REBUILT":
            name = parts[1]
            new_leader = parts[2]

            reply = rebuilt(peers, dht, name, new_leader)
 
        elif cmd == "DEREGISTER":
            name = parts[1]

            reply = deregister(peers, name)
 
        elif cmd == "TEARDOWN-DHT":
            name = parts[1]

            reply = teardown(dht, name)
 
        elif cmd == "TEARDOWN-COMPLETE":
            name = parts[1]
            
            reply = teardown_complete(peers, dht, name)
 
        else:
            reply = "FAILURE"

        print(f"[manager] send to {addr}: {reply}")
        sock.sendto(reply.encode(), addr)


if __name__ == "__main__":
    main()
