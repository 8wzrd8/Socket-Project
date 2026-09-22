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

    #TODO
    #we would start threading here and use a p2p listener 

    reply = messageManager(f"REGISTER|{name}|{ip}|{mport}|{pport}")
    print(f"[peer {name}] register -> {reply}")


    

def setup(args):
    #setup has args n, year
    n = int(args[0])
    year = args[1]

    reply = messageManager(f"SETUP-DHT|{state['name']}|{n}|{year}")
    print(f"[peer {state['name']}] setup-dht -> {reply}")

    if reply.startswith("SUCCESS"):
        #TODO
        #parse tuples
        #build the dht
        pass
    


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