import socket
import sys
import random

def register(peers, name, ip, mport, pport):

    #duplicate r
    if name in peers:
        return "FAILURE"

    #need to check which ports are alr in use
    #save them in here
    used = set()
    for p in peers.values():
        used.add((p["ip"], p["mport"]))
        used.add((p["ip"], p["pport"]))

    if (ip, mport) in used or (ip, pport) in used:
        return "FAILURE"


    #we made it to here sowe are fine to store the peer
    peers[name] = {"ip": ip, "mport": mport, "pport": pport, "state": "Free"}
    return "SUCCESS"



def setup(peers, dht, name, n, year):

    #requester is not registered or is busy or n >=3 or dht already exists 
    if name not in peers or peers[name]["state"] != "Free" or n < 3 or dht["exists"]:
        return "FAILURE"

    #add all free peers
    free = []
    for p in peers:
        if p != name and peers[p]["state"] == "Free":
            free.append(p)

    if len(free) < (n - 1):
        return "FAILURE"

    #has to be random from the desc
    subset = random.sample(free, n - 1)

    #requester is leader, all others are just indht
    peers[name]["state"] = "Leader"
    for p in subset:
        peers[p]["state"] = "InDHT"

    members = [name] + subset
    dht.update(exists=True, building=True, leader=name, members=members)

    #"name,ip,p-port"
    parts = []
    for m in members:
        parts.append(f"{m},{peers[m]['ip']},{peers[m]['pport']}")
    tuple = ";".join(parts)

    return f"SUCCESS|{tuple}"




def complete(dht, name):
    #has to be sent by the leader to approve
    if name != dht["leader"]:
        return "FAILURE"

    dht["building"] = False
    return "SUCCESS"



def main():

    if len(sys.argv) != 2:
        sys.exit(1)

    port = int(sys.argv[1])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", port))

    peers = {}
    dht = {"exists": False, "building": False, "leader": None, "members": []}

    #manager loops forever waiting for messeages from peers
    while True:
        result = sock.recvfrom(4096)
        data = result[0]
        addr = result[1]

        msg = data.decode().strip()
        print(f"[manager] recv from {addr}: {msg}")

        parts = msg.split("|")
        cmd = parts[0].upper()

        #spec says that we must respond failure if we are
        #already building a DHT
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

        else:
            reply = "FAILURE"

        #send response back to where it came from 
        print(f"[manager] send to {addr}: {reply}")
        sock.sendto(reply.encode(), addr)




if __name__ == "__main__":
    main()
