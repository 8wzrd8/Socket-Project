import socket
import sys
import threading
import csv
from sympy import nextprime

state = {
    "name": None,
    "msock": None,      
    "psock": None,      
    "address": None,    
    "id": None,       
    "n": None,         
    "neighbor": None,    
    "table": {},         
    "tuples": [],       
    "leader": False,
    "counts": {},        
}


def messageManager(message):
    #basically the same as the code in manager
    state["msock"].sendto(message.encode(), state["address"])
    data, _ = state["msock"].recvfrom(8192)
    return data.decode().strip()


def parseTuples(tuples):
    #takes a string such as:
    #"Sarah,127.0.0.1,6001;Ali,127.0.0.1,6002;Prateek,127.0.0.1,6003"
    #converts it too a list (name, ip, pport)
    result = []
    for chunk in tuples.split(";"):
        name, ip, pport = chunk.split(",")
        result.append((name, ip, int(pport)))

    return result


def serializeTuples(tuples):
    #inverse of parseTuples
    parts = []
    for name, ip, pport in tuples:
        parts.append(f"{name},{ip},{pport}")

    return ";".join(parts)



def register(args):
    #register has args name, ip, mport and pport
    name = args[0]
    ip = args[1]
    mport = int(args[2])
    pport = int(args[3])
    state["name"] = name

    state["msock"] = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    state["msock"].bind(("", mport))

    state["psock"] = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    state["psock"].bind(("", pport))

    #start listeing for peere to peer messages 
    threading.Thread(target=p2plistener, daemon=True).start()

    reply = messageManager(f"REGISTER|{name}|{ip}|{mport}|{pport}")
    print(f"[peer {name}] register -> {reply}")


def send_store(target_id, pos, fields):
    #forward a record one hop along the ring to our right neighbor.
    #used both by the leader (initial distribution) and by any peer
    #that receives a STORE meant for someone further down the ring.
    record_str = ",".join(fields)
    msg = f"STORE|{target_id}|{pos}|{record_str}"
    _, ip, pport = state["neighbor"]
    state["psock"].sendto(msg.encode(), (ip, pport))


def p2p_setid(idn, ringn, tuples_str):
    #"On receipt of a set-id, peer_i sets its identifier to i and the ring
    #size to n. It stores the 3-tuple of peer_(i+1) mod n to use as the
    #address of its right neighbour."
    state["id"] = idn
    state["n"] = ringn
    tuples = parseTuples(tuples_str)
    state["tuples"] = tuples
    state["neighbor"] = tuples[(idn + 1) % ringn]

    print(f"[peer {state['name']}] got SET-ID: id={idn} n={ringn} "
          f"neighbor={state['neighbor'][0]}")


def p2p_store(target_id, pos, fields):
    #"A storm-event record is being routed around the ring toward whichever
    #node is responsible for storing it. target_id is that destination
    #node's ring identifier; pos is the exact slot in ITS local hash table."
    if target_id == state["id"]:
        state["table"][pos] = fields
        print(f"[peer {state['name']}] stored record "
              f"event_id={fields[0]} at pos={pos}")
    else:
        send_store(target_id, pos, fields)


def p2plistener():
    #Intended for a future shutdown protocol
    continueListening = True
    while continueListening:
        #recvfrom blocks until some other peer sends a datagram
        peermsg, peeraddr = state["psock"].recvfrom(65536)
        msg = peermsg.decode().strip()   
        parts = msg.split("|")
        cmd = parts[0].upper()

        if cmd == "SET-ID":
            idn = int(parts[1])
            ringn = int(parts[2])
            tuples_str = parts[3]

            p2p_setid(idn, ringn, tuples_str)

        elif cmd == "STORE":
            target_id = int(parts[1])
            pos = int(parts[2])
            fields = parts[3].split(",")
            
            p2p_store(target_id, pos, fields)

        else:
            print(f"[peer {state['name']}] unknown p2p message: {msg}")


def buildDHT(tuples, n, year):
    # only the peer that issued setup-dht (the leader) runs this
    state["tuples"] = tuples
    state["n"] = n
    state["id"] = 0
    state["leader"] = True
    state["neighbor"] = tuples[1 % n]
    state["counts"] = {i: 0 for i in range(n)}

    tuplesString = serializeTuples(tuples)

    #TODO
    # 1.    For in range i = 1 .. n-1, tell peer_i "you are node i". We send it the full
    #       tuple list too, so each peer can independently compute its own right
    #       neighbor the same way we just did above.
    #       
    #
    #       at the top of this loop we start by getting the name, ip and port from the tuple
    #       here is how you would get the name name_i = tuples[i][0]
    #       follow the same format as all th other msg(s) i.e
    #       f"SET-ID|{i}|{n}|{tuplesString}"
    #       We then need to encode this meesage, and send this message to the psock
    #       of the ip_i and port_i you just got
    #
    #       At the very end of this loop include: print(f"[peer {state['name']}] sent SET-ID (id={i}) to {name_i}")
    

    # 2. load the dataset and distribute records around the ring
    filename = f"details-{year}.csv"
    try:
        with open(filename, newline="") as f:
            reader = list(csv.reader(f))
    except FileNotFoundError:
        print(f"[peer {state['name']}] ERROR: cannot find {filename}")
        return

    #here are some useful things 
    rows = reader[1:]
    # l = len(rows) instead of len(rows) - 1 because rows already skips the header row
    l = len(rows)
    s = nextprime(2 * l)

    #TODO

    #   0           1       2       3           4           5       6       7               8                   9               10              11              12              13
    #   event_id    state   year    month_name  event_type  cz_type cz_name injuries_direct injuries_indirect   deaths_direct   deaths_indirect damage_property damage_crops    tor_f_scale

    # pos   =   event_id mod s
    #       =   event_id mod 449
    # id    =   pos mod n
    #       =   pos mod (≥3?)

    neighborstr = ''

    for rowindex in rows:
        readeventid = reader[rowindex][0]
        calculatehash1 = readeventid % s
        calculatehash2 = calculatehash1 % state['n']

        if calculatehash2 == state['id']:
            # Store the row locally
        else:
            # Pass to the neighbor
            # Unless the neighbor is the leader.
            # By the time it reaches peer(n-1), that peer should not need to pass anything on.




    #rows is a list of lists
    #here is what the first list looks like
    #rows[0] == ['10096222', 'OKLAHOMA', '1950', 'April', 'Tornado', 'C', 'WASHITA', '0', '0', '0', '0', '250K', '0', 'F3']
    #so if we wanted the eventid, we would do rows[0][0]
    #since we want to work with one list at a time we could do this:

    #for fields in rows:
    # we set get the event id and save it in a var
    # then from the project spec:
    #
    # "The first hash function computes a position, pos, as the event id field modulo the hash table size... 
    # The second hash function is the position modulo the ring size n. pos = event id mod s, traget_id = pos mod n."
    #
    # increment state["counts"][target_id]
    #
    # 
    #if target_id == 0:
    #         state["table"][pos] = fields
    # else:
    #   this code is the same as the store branch in p2plistener
    #

    print(f"[peer {state['name']}] record distribution:")
    for i in range(n):
        print(f"    node {i}: {state['counts'][i]} records")

    reply = messageManager(f"DHT-COMPLETE|{state['name']}")
    print(f"[peer {state['name']}] dht-complete -> {reply}")


def setup(args):
    #setup-dht has args n, year
    n = int(args[0])
    year = args[1]

    reply = messageManager(f"SETUP-DHT|{state['name']}|{n}|{year}")
    print(f"[peer {state['name']}] setup-dht -> {reply}")

    if reply.startswith("SUCCESS"):
        tuples = parseTuples(reply.split("|", 1)[1])
        buildDHT(tuples, n, year)


def main():
    if len(sys.argv) != 3:
        print("usage: python3 peer.py <manager-ip> <manager-port>")
        sys.exit(1)

    state["address"] = (sys.argv[1], int(sys.argv[2]))

    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break

        if not line:
            continue

        #ex: parts = ["register", "Sarah", "127.0.0.1", "6001", "6002"]
        #cmd = "register", args = ["Sarah", "127.0.0.1", "6001", "6002"]
        parts = line.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd == "register":
            register(args)

        elif cmd == "setup-dht":
            setup(args)

        elif cmd == "quit":
            break

        else:
            print(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
