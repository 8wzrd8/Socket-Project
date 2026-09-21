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

def register(args):
    #TODO
    return -1
    

def setup(args):
    #TODO
    return -1
    
    


def main():
    if len(sys.argv) != 3:
            sys.exit(1)

    state["address"] = (sys.argv[1], int(sys.argv[2]))

    #parsing the cmds
    while True:
         line = input("> ").strip()
         if not line:
              continue

         #ex parts = ["register", "Sarah", "127.0.0.1", "6001", "6002"]
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