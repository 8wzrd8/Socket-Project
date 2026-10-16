import socket
import sys
import threading
import csv
import random
from enum import IntEnum
from sympy import nextprime

class DHeader(IntEnum):
    EVENT_ID = 0
    STATE = 1
    YEAR = 2
    MONTH_NAME = 3
    EVENT_TYPE = 4
    CZ_TYPE = 5
    CZ_NAME = 6
    INJURIES_DIRECT = 7
    INJURIES_INDIRECT = 8
    DEATHS_DIRECT = 9
    DEATHS_INDIRECT = 10
    DAMAGE_PROPERTY = 11
    DAMAGE_CROPS = 12
    TOR_F_SCALE = 13

state = {
    "name": None,
    "msock": None,
    "psock": None,
    "address": None,
    "id": None,
    "n": None,
    "s": None,
    "neighbor": None,
    "table": {},
    "tuples": [],
    "leader": False,
    "counts": {},
    "query_event": threading.Event(),
    "query_result": None,
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
    state["ip"] = ip
    state["pport"] = pport

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


def p2p_setdhtsize(size_prime_num):
    state["s"] = size_prime_num

    if state["id"] == 0:
        return
 
    msg = f"SET-DHT-SIZE|{size_prime_num}"
    _, ip, pport = state["neighbor"]
    state["psock"].sendto(msg.encode(), (ip, pport))



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

def finish_leave(new_leader_name):
    reply = messageManager(f"DHT-REBUILT|{state['name']}|{new_leader_name}")
    print(f"[peer {state['name']}] dht-rebuilt -> {reply}")
 
    #we've left the dht reset back to a plain outside-observer peer
    state["leader"] = False
    state["id"] = None
    state["n"] = None
    state["s"] = None
    state["neighbor"] = None
    state["tuples"] = []
    state["table"] = {}

def p2p_reset_id(new_id, new_n, initiator, tuples_str):
    initiator_name, initiator_ip, initiator_pport = initiator
 
    if state["name"] == initiator_name:
        #the lap has come all the way back around 
        new_tuples = parseTuples(tuples_str)
        finish_leave(new_tuples[0][0])
        return
 
    state["id"] = new_id
    state["n"] = new_n
    state["leader"] = (new_id == 0)
    new_tuples = parseTuples(tuples_str)
    state["tuples"] = new_tuples
 
    print(f"[peer {state['name']}] got RESET-ID: id={new_id} n={new_n}")
 
    if new_id == new_n - 1:
        #last member of the new ring: our real neighbor going forward is
        #the new leader, but THIS message needs to close the loop by going
        #directly back to whoever started the leave, not around the ring
        state["neighbor"] = new_tuples[0]
        next_ip, next_pport = initiator_ip, initiator_pport
    else:
        state["neighbor"] = new_tuples[(new_id + 1) % new_n]
        next_ip, next_pport = state["neighbor"][1], state["neighbor"][2]
 
    msg = f"RESET-ID|{new_id + 1}|{new_n}|{initiator_name},{initiator_ip},{initiator_pport}|{tuples_str}"
    state["psock"].sendto(msg.encode(), (next_ip, next_pport))


def p2p_join_request(joiner_name, joiner_ip, joiner_pport):
    #only ever received by whoever is currently the leader
    old_n = state["n"]
    new_n = old_n + 1
    new_tuples = state["tuples"] + [(joiner_name, joiner_ip, joiner_pport)]
    tuples_str = serializeTuples(new_tuples)

    for i in range(old_n):
        name_i, ip_i, port_i = state["tuples"][i]
        if name_i == state["name"]:
            continue
        msg = f"SET-ID|{i}|{new_n}|{tuples_str}"
        state["psock"].sendto(msg.encode(), (ip_i, port_i))
 
    #tell the new peer its id 
    msg = f"SET-ID|{new_n - 1}|{new_n}|{state['s']}|{tuples_str}"
    state["psock"].sendto(msg.encode(), (joiner_ip, joiner_pport))
 
    msg = f"SET-DHT-SIZE|{state['s']}"
    state["psock"].sendto(msg.encode(), (joiner_ip, joiner_pport))
 
    #our own id/neighbor are unaffected 
    state["n"] = new_n
    state["tuples"] = new_tuples
 
    print(f"[peer {state['name']}] {joiner_name} joined as id={new_n - 1}; "f"ring size now {new_n}")
    reply = messageManager(f"DHT-REBUILT|{joiner_name}|{state['name']}")
    print(f"[peer {state['name']}] dht-rebuilt -> {reply}")


def p2p_teardown(initiator_name, mode):
    #generic teardown
    state["table"] = {}
    print(f"[peer {state['name']}] cleared local table (teardown)")
 
    if state["name"] == initiator_name:
        if mode == "FULL":
            reply = messageManager(f"TEARDOWN-COMPLETE|{state['name']}")
            print(f"[peer {state['name']}] teardown-complete -> {reply}")
            
        else:
                old_tuples = state["tuples"]
                my_id = state["id"]
                n = state["n"]
             
                new_tuples = []
                for k in range(1, n):
                    new_tuples.append(old_tuples[(my_id + k) % n])
             
                new_n = n - 1
                tuples_str = serializeTuples(new_tuples)
             
                #our old right neighbor becomes id=0
                msg = f"RESET-ID|0|{new_n}|{state['name']},{state['ip']},{state['pport']}|{tuples_str}"
                _, ip, pport = state["neighbor"]
                state["psock"].sendto(msg.encode(), (ip, pport))
        return
 
    msg = f"TEARDOWN|{initiator_name}|{mode}"
    _, ip, pport = state["neighbor"]
    state["psock"].sendto(msg.encode(), (ip, pport))

def p2p_find_event(event_id, requester, visited):
    #hot potato, every node that handles this message gets
    #appended to visited (the id-seq trace), regardless of whether it
    #turns out to be the owner or just another random hop along the way
    visited = visited + [state["id"]]
    visited_csv = ",".join(str(v) for v in visited)
 
    req_name, req_ip, req_pport = requester
    
    pos = event_id % state["s"]
    target_id = pos % state["n"]
 
    if target_id == state["id"]:
        #the node responsible for this pos
        #reply back to whoever originally asked
        fields = state["table"].get(pos)
        #gotta love python
        if fields is not None and int(fields[DHeader.EVENT_ID]) == event_id:
            record_csv = ",".join(fields)
            msg = f"FIND-RESULT|SUCCESS|{visited_csv}|{record_csv}"
        else:
            msg = f"FIND-RESULT|FAILURE|{visited_csv}|{event_id}"
 
        state["psock"].sendto(msg.encode(), (req_ip, req_pport))
        return
 
    #if we made it here then its not us, bounce to a random node we havent probed yet
    unvisited = [i for i in range(state["n"]) if i not in visited]
    if unvisited:
        next_id = random.choice(unvisited)
    else:
        #cant happen (the owner is always reachable within n hops)
        #but fall back to addressing the owner directly rather than deadlock
        next_id = target_id
 
    next_name, next_ip, next_pport = state["tuples"][next_id]
    msg = f"FIND-EVENT|{event_id}|{req_name},{req_ip},{req_pport}|{visited_csv}"
    state["psock"].sendto(msg.encode(), (next_ip, next_pport))
    print(f"[peer {state['name']}] forwarding find-event for {event_id} "
          f"to {next_name} (id={next_id})")


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

        elif cmd == "SET-DHT-SIZE":
            s = int(parts[1])

            p2p_setdhtsize(s)

        elif cmd == "STORE":
            target_id = int(parts[1])
            pos = int(parts[2])
            fields = parts[3].split(",")
            
            p2p_store(target_id, pos, fields)

        elif cmd == "FIND-EVENT":
            event_id = int(parts[1])
            req_name, req_ip, req_pport = parts[2].split(",")
            requester = (req_name, req_ip, int(req_pport))
            visited_csv = ""
            if len(parts) > 3:
                visited_csv = parts[3]

            visited = []
            if visited_csv:
                for v in visited_csv.split(","):
                    visited.append(int(v))

            p2p_find_event(event_id, requester, visited)
                
        elif cmd == "FIND-RESULT":
            #TODO
            status = parts[1]
            id_seq = parts[2]
 
            if status == "SUCCESS":
                fields = parts[3].split(",")
                state["query_result"] = ("SUCCESS", id_seq, fields)
            else:
                state["query_result"] = ("FAILURE", id_seq, parts[3])
 
            state["query_event"].set()

        elif cmd == "TEARDOWN":
            initiator_name = parts[1]
            mode = parts[2]

            p2p_teardown(initiator_name, mode)

        elif cmd == "RESET-ID":
            new_id = int(parts[1])
            new_n = int(parts[2])
            init_name, init_ip, init_pport = parts[3].split(",")
            initiator = (init_name, init_ip, int(init_pport))
            tuples_str = parts[4]

            p2p_reset_id(new_id, new_n, initiator, tuples_str)

        elif cmd == "JOIN-REQUEST":
            joiner_name, joiner_ip, joiner_pport = parts[1].split(",")

            p2p_join_request(joiner_name, joiner_ip, int(joiner_pport))

        else:
            print(f"[peer {state['name']}] unknown p2p message: {msg}")


def buildDHT(tuples, n, year):
    # only the peer that issued setup-dht (the leader) runs this
    state["n"] = n
    state["id"] = 0
    state["leader"] = True
    state["tuples"] = tuples
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
    for i in range(1, n):
        msgtuplelist = f"SET-ID|{i}|{n}|{tuplesString}"
        # ipaddress is coded as (ipv4, port)
        peername = tuples[i][0]
        peerip = tuples[i][1]
        peerport = tuples[i][2]
        state["psock"].sendto(msgtuplelist.encode(), (peerip, peerport))

        print(f"[peer {state['name']}] sent SET-ID (id={i}) to {peername}")
    

    # 2. load the dataset and distribute records around the ring

    #   0           1       2       3           4           5       6       7               8                   9               10              11              12              13
    #   event_id    state   year    month_name  event_type  cz_type cz_name injuries_direct injuries_indirect   deaths_direct   deaths_indirect damage_property damage_crops    tor_f_scale

    # pos   =   event_id mod s
    #       =   event_id mod 449
    # id    =   pos mod n
    #       =   pos mod (≥3?)

    writetotable = True
    writetofile = False
    filename = f"details-{year}.csv"
    peerdhtname = 'peer-' + str(state['id']) + '-' + str(filename)

    try:
        # Modified to open multiple files at the same time.
        # with open(filename, newline="") as f, open(peerdhtname, "w") as writestormfile:
        with open(filename, newline="") as f:
            reader = list(csv.reader(f))
            # writer = csv.writer(writestormfile)

            #here are some useful things
            rows = reader[1:]
            # l = len(rows) instead of len(rows) - 1 because rows already skips the header row
            l = len(rows)
            s = nextprime(2 * l)

            # Store term s at every peer for query-dht utility
            state["s"] = s
            msgstores = f"SET-DHT-SIZE|{s}"
            neighborname, neighborip, neighborport = state["neighbor"]
            # tuple state["neighbor"]
            state["psock"].sendto(msgstores.encode(), (neighborip, neighborport))
            print(f"[peer {state['name']}] sent SET-DHT-SIZE ({s}) to {neighborname}")

            for rowindex in rows:
                readeventid = int(rowindex[DHeader.EVENT_ID])
                calculatehash1 = readeventid % s
                calculatehash2 = calculatehash1 % n # Determines the appropriate peer-id for this entry

                # Ex: With a list of 223 events, 5 peers, id is 2, the peer should store 45 entries
                # Calculate count ahead instead of needing peers to message their record size
                state["counts"][calculatehash2] += 1
                if calculatehash2 == state['id']:
                    # Store the row locally within state["table"]
                    if writetotable:
                        state["table"][calculatehash1] = rowindex

                    # Store the row on a local csv file to the peer
                    # The format of the written file name is "peer-{this-peer-id}-details-{storm-data-year}.csv"
                    if writetofile:
                        locallist = rowindex
                        # writer.writerow(locallist)
                else:
                    # Pass to the neighbor
                    # Unless the neighbor is the leader.
                    # By the time it reaches peer(n-1), that peer should not need to pass anything on.

                    # Calculatehash2 already identifies the intended recipient
                    # Pass it through a message to the neighbor
                    send_store(calculatehash2, calculatehash1, rowindex)
    except FileNotFoundError:
        print(f"[peer {state['name']}] ERROR: cannot find {filename}")
        return



    #TODO


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


def findevent(args):
    # find-event has args event_id, list_og_sender_addr, list_seq_id
    event_id = args[0]
    request_addr_ip = args[1][0]
    request_addr_pport = args[1][1]
    list_seq_id = args[2]

    s = int(state['n'])
    n = int(state['n'])

    calculatehash1 = readeventid % s #TODO
    calculatehash2 = calculatehash1 % n


def sendquery(args):
    #query-dht has args event_id
    event_id = int(args[0])
 
    reply = messageManager(f"QUERY-DHT|{state['name']}")
    print(f"[peer {state['name']}] query-dht -> {reply}")
 
    if not reply.startswith("SUCCESS"):
        return
 
    target_name, target_ip, target_pport = reply.split("|", 1)[1].split(",")
    target_pport = int(target_pport)
 
    state["query_event"].clear()
    state["query_result"] = None
 
    #kick off the hot-potato find-event walk at the random member the manager pointed us to
    msg = f"FIND-EVENT|{event_id}|{state['name']},{state['ip']},{state['pport']}|"
    state["psock"].sendto(msg.encode(), (target_ip, target_pport))
 
    got_result = state["query_event"].wait(timeout=10)
    if not got_result:
        print(f"[peer {state['name']}] query timed out waiting for a response")
        return
 
    status, id_seq, payload = state["query_result"]
    if status == "SUCCESS":
        fields = payload
        print(f"[peer {state['name']}] record found:")
        for member in DHeader:
            print(f"    {member.name}: {fields[member.value]}")
        print(f"[peer {state['name']}] id-seq: {id_seq}")
    else:
        print(f"Storm event {event_id} not found in the DHT.")


def leave_dht(args):
    #msg format
    reply = messageManager(f"LEAVE-DHT|{state['name']}")
    print(f"[peer {state['name']}] leave-dht -> {reply}")
    
    if reply == "SUCCESS":
        msg = f"TEARDOWN|{state['name']}|LEAVE"
        _, ip, pport = state["neighbor"]
        state["psock"].sendto(msg.encode(), (ip, pport))
 
def join_dht(args):
    #msg format
    reply = messageManager(f"JOIN-DHT|{state['name']}")
    print(f"[peer {state['name']}] join-dht -> {reply}")
 
    if not reply.startswith("SUCCESS"):
        return
    
    leader_name, leader_ip, leader_pport = reply.split("|", 1)[1].split(",")
 
    msg = f"JOIN-REQUEST|{state['name']},{state['ip']},{state['pport']}"
    state["psock"].sendto(msg.encode(), (leader_ip, int(leader_pport)))
 
def teardown_dht(args):
    #msg format
    reply = messageManager(f"TEARDOWN-DHT|{state['name']}")
    print(f"[peer {state['name']}] teardown-dht -> {reply}")
 
    if reply == "SUCCESS":
        msg = f"TEARDOWN|{state['name']}|FULL"
        _, ip, pport = state["neighbor"]
        state["psock"].sendto(msg.encode(), (ip, pport))
 
def deregister_cmd(args):
    #msg format
    reply = messageManager(f"DEREGISTER|{state['name']}")
    print(f"[peer {state['name']}] deregister -> {reply}")
 
    if reply == "SUCCESS":
        sys.exit(0)

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

        elif cmd == "query-dht":
            sendquery(args)

        elif cmd == "leave-dht":
            leave_dht(args)
 
        elif cmd == "join-dht":
            join_dht(args)
 
        elif cmd == "teardown-dht":
            teardown_dht(args)
 
        elif cmd == "deregister":
            deregister_cmd(args)
            
        elif cmd == "quit":
            break

        else:
            print(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
