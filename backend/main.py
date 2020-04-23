import socket
import select
import signal
from _thread import *
import threading
from concurrent.futures import ThreadPoolExecutor

import world_ups_pb2
import ups_amazon_pb2
from db import *
from utility import *

world_host = 'vcm-14675.vm.duke.edu'
world_port = 12345
amazon_host = ''
amazon_port = ''
world = 0

def handle_world(Cw, Ca, Cdb):
     while True:
          with world_lock:
               UResponse = world_ups_recv(Cw)
               t1 = threading.Thread(target = process_UResponse, args=(UResponse, Cw, Ca, Cdb))
               t1.start()
               t1.join()

def handle_amazon(Cw, Ca, Cdb):
     while True:
          with amazon_lock:
               UA_Response = amazon_ups_recv(Ca)
               t2 = threading.Thread(target = process_UA_Response, args = (UA_Response, Cw, Ca, Cdb))
               t2.start()
               t2.join()


def main():
     # connect to world simulator
     Cw = connect_to_world(world_host, world_port)
     # connect to databse
     Cdb = connect_to_db()
     drop_if_exists(Cdb)
     create_table(Cdb)
     # get worldid and tell amazon using UA_Connect
     world_id = get_world_id(Cw, world, Cdb)
     ua_con = UA_Connect()
     ua_con.worldid = world_id
     Ca = connect_to_amazon(amazon_host, amazon_port)
     send_msg(Ca, ua_con)

     # multi threads for world and amazon communications
     t1 = threading.Thread(target = handle_world, args = (Cw, Ca, Cdb))
     t2 = threading.Thread(target = handle_amazon, args = (Cw, Ca, Cdb))

     t1.start()
     t2.start()
     t1.join()
     t2.join()

     Cw.close()
     Ca.close()

# main
if __name__ == '__main__':
    main()