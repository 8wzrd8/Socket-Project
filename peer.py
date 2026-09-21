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