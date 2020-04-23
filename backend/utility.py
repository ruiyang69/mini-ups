import socket
import time
import select
import smtplib, ssl
from db import *
import world_ups_pb2 as wupb
import ups_amazon_pb2 as uapb
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint


# email set up
smtp_server = "smtp.gmail.com"
email_port = 587 
sender_email = "patrickyr08@gmail.com"
email_password ='Impatrick1008!'

# thread locks
world_lock = threading.Lock()
amazon_lock = threading.Lock()
seq_lock = threading.Lock()
seqnum = 0


# generic send message function
def send_msg(socket, msg):
    print('sending: "')
    print(msg)
    rqst = msg.SerializeToString()
    _EncodeVarint(socket.send, len(rqst), None)
    socket.send(rqst)
    print('"send succeeds.')


# Recv function between UPS and world
def world_ups_recv(sock, isUConnect=False):
    all_data = b''
    data = sock.recv(4)
    if not data:
        print('connection to world is closed')
    data_len, new_pos = _DecodeVarint32(data, 0)
    all_data += data[new_pos:]

    data_left = data_len - len(all_data)
    while True:
        data = sock.recv(data_left)
        all_data += data
        data_left -= len(data)

        if data_left <= 0:
            break
        
    if isUConnect:
        msg = wupb.UConnected()
        msg.ParseFromString(all_data)
        return msg

    msg = wupb.UResponses()
    msg.ParseFromString(all_data)
    return msg


# socket connect to world
def connect_to_world(hostname, port):
    print('Connecting to world...')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((hostname, port))
    if s is not None:
        print('Connection to world succeeds')
    return s


# get worldid
def get_world_id(Cw, world, Cdb):
    UCon_msg = wupb.UConnect()
    UCon_msg.isAmazon = False
    if world > 0:
        UCon_msg.worldid = world
    else:
        for i in range(20):
            t = UCon_msg.trucks.add()
            t.id = i
            t.x = 99
            t.y = 99
            insert_truck(Cdb, i, 'idle')

    send_msg(Cw, UCon_msg)
    res = world_ups_recv(Cw, True)
    print('World connection status: ', res.result)
    return res.worldid


# process UResponse
def process_UResponse(UResponse, Cw, Ca, Cdb):
    global seqnum
    to_world = wupb.UCommands()
    to_amazon = uapb.UA_Responses()

    print('checking UFinished')
    for ufin in UResponse.completions:
        to_world.ack.append(ufin.seqnum)
        if ufin.status == 'arrive warehouse':
            print('a truck has arrived at warehouse, notify Amazon')
            UA_TruckArrived = to_amazon.truckArrived
            UA_TruckArrived.truckid = ufin.trackid
            with seq_lock:
                UA_TruckArrived.seqnum = seqnum
                seqnum += 1
            
            update_truck_status(Cdb, ufin.trackid, 'loading')

    
    print('checking UDeliveryMade')
    for udel in UResponse.delivered:
        to_world.ack.append(udel.seqnum)
        UA_Delivered = to_amazon.delivered
        UA_Delivered.packageid = udel.packageid
        UA_Delivered.truckid = udel.truckid
        with seq_lock:
            UA_Delivered.seqnum = seqnum
            seqnum += 1
        
        update_package_status(Cdb, ufin.packageid, 'delivered')

    print('checking UTruck')
    for utru in UResponse.truckstatus:
        to_world.acks.append(utruck.seqnum)    
        update_package_pos(Cdb, utru.truckid, utru.x, utru.y)

    send_msg(Cw, to_world)    
    send_msg(Ca, to_amazon)



# connection to Amazon
def connect_to_amazon(hostname, port):
    print('Connecting to Amazon...')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((hostname, port))
    return s


def amazon_ups_recv(sock):
    all_data = b''
    data = sock.recv(4)
    if not data:
        print('connection to amazomn is closed')
    data_len, new_pos = _DecodeVarint32(data, 0)
    all_data += data[new_pos:]

    data_left = data_len - len(all_data)
    while True:
        data = sock.recv(data_left)
        all_data += data
        data_left -= len(data)

        if data_left <= 0:
            break

    msg = uapb.UA_Commands()
    msg.ParseFromString(all_data)
    return msg