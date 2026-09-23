import socket
import sys
import threading
import csv

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
    "count": {},
}


#both isprime and nextprime are in the
#sympy library, i didnt want to use a third
#party library so i made them myself 
def isprime(n):
    #all from def of a prime
    if n < 2:
        return False
    
    if n == 2:
        return True
    
    if n % 2 == 0:
        return False

    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2

    return True

def nextprime(n):
    #probaly not the best way to do this
    current = n + 1
    while not isprime(n):
        current += 1

    return current

def messageManager(message):
    #basically the same as the code in manager
    state["msock"].sendto(message.encode(), state["address"])
    result = state["msock"].recvfrom(4096)
    data = result[0]
    return data.decode().strip()

def parseTuples(tuples):
    #takes a tuple such as:
    #"Sarah,127.0.0.1,6001;Ali,127.0.0.1,6002;Prateek,127.0.0.1,6003"
    #converts it too a list (name, ip, pport)
    list = []
    for chunk in tuples.split(";"):
        name, ip, pport = chunk.split(",")
        list.append((name, ip, int(pport)))

    return list


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

# p2plistener guide
# (this code will all be nested in a while True)
# there are three main parts to this code
# 1. recvfrom block until some other peer sends a datagram
# 2.    if cmd == "SET-ID":
#
#       "On receipt of a set-id,
#       peer_i sets its identifier to i and the ring size to n. It
#       stores the 3-tuple of peer_(i+1) mod n to use as the address
#       of its right neighbour."
#
#       Once finished, print(f"[peer {state['name']}] got SET-ID: id={id} n={n} "f"neighbor={right[0]}")

# 3. elif cmd == "STORE"
#
#       "A storm-event record is being
#       routed around the ring toward whichever node is responsible
#       for storing it. target_id is that destination node's ring
#       identifier; pos is the exact slot in ITS local hash table.""
#
#       Check if our target id is equal to the state id, if true then print
#       the message below if false forward the record to our neighbor 
#
#       Once finished, print print(f"[peer {state['name']}] stored record "f"event_id={fields[0]} at pos={pos}")

def p2plistener():
    #TODO
    pass

# buildDHT guide
# the only peer running this command is the one who issued setup (the peer leader )
# assign state[] values given the args and knowing that it is a leader (including counts)
#
# there are three main parts to this code
# 1.    For i = 1 .. n-1, tell peer_i "you are node i". We send it the full
#       tuple list too, so each peer can independently compute its own right
#       neighbor the same way we just did above.
#       At the very end of this loop include: print(f"[peer {state['name']}] sent SET-ID (id={i}) to {name_i}")
#
# 2. load the dataset and share records 
#
#    filename = f"details-{year}.csv"
#    try:
#        with open(filename, newline="") as f:
#            reader = list(csv.reader(f))
#    except FileNotFoundError:
#        print(f"[peer {state['name']}] ERROR: cannot find {filename}")
#        return
#
#    Now we need to get the data from the csv
#    skip the header, then for fields in rows:
#    process id, pos, tagetid, and counts @ that targetid
#    if target_id == 0:
#            state["table"][pos] = fields
#    else:
#       this code is the same as the store branch in p2plistener
#   
#   
#
# 3. print configuration & finish
#
#    print(f"[peer {state['name']}] record distribution:")
#    for i in range(n):
#        print(f"    node {i}: {state['counts'][i]} records")
#     
#    reply = messageManager(f"DHT-COMPLETE|{state['name']}")
#    print(f"[peer {state['name']}] dht-complete -> {reply}")

def buildDHT(tuples, n, year):
    #TODO
    pass


    

def setup(args):
    #setup has args n, year
    n = int(args[0])
    year = args[1]

    reply = messageManager(f"SETUP-DHT|{state['name']}|{n}|{year}")
    print(f"[peer {state['name']}] setup-dht -> {reply}")

    if reply.startswith("SUCCESS"):
        #parse tuples
        tuples = parseTuples(reply.split("|", 1)[1])
        #build the dht
        buildDHT(tuples, n, year)
    


def main():
    if len(sys.argv) != 3:
            sys.exit(1)

    state["address"] = (sys.argv[1], int(sys.argv[2]))

    #parsing the cmds
    while True:
         line = input("> ").strip()
         if not line:
              continue

         #ex 
         #parts = ["register", "Sarah", "127.0.0.1", "6001", "6002"]
         #cmd = "register"
         #args = ["Sarah", "127.0.0.1", "6001", "6002"]

         parts = line.split()
         cmd =parts[0].lower()
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