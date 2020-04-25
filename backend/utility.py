import socket
import time
import threading
import select
import smtplib, ssl

from db import *
import world_ups_pb2 as wupb
import ups_amazon_pb2 as uapb
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint

C = sqlite3.connect(database="../db.sqlite3")

# email set up
smtp_server = "smtp.gmail.com"
email_port = 587 
sender_email = "patrickyr08@gmail.com"
email_password ='1008Ollie'

# thread locks
world_lock = threading.Lock()
amazon_lock = threading.Lock()
seq_lock = threading.Lock()
whnum_lock = threading.Lock()
seqnum = 0
whnum = 0

# generic send message function
def send_msg(socket, msg):
    print('sending: ')
    print(msg)
    rqst = msg.SerializeToString()
    _EncodeVarint(socket.send, len(rqst), None)
    socket.send(rqst)
    # print('"send succeeds.')


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
        for i in range(5):
            t = UCon_msg.trucks.add()
            t.id = i
            t.x = 10
            t.y = 10
            insert_truck(Cdb, i, 'idle')

    send_msg(Cw, UCon_msg)
    res = world_ups_recv(Cw, True)
    print('World connection status: ', res.result)
    return res.worldid


# process UResponse
def process_UResponse(UResponse, Cw, Ca, Cdb):
    global seqnum
    global whnum
    send_world = False
    send_amazon = False

    to_world = wupb.UCommands()
    to_amazon = uapb.UA_Responses()

    print(UResponse)
    for ufin in UResponse.completions:
        print('receive UFinished from the world')
        send_world = True
        to_world.acks.append(ufin.seqnum)

        print('status:' + ufin.status)
        if ufin.status == 'arrive warehouse' or ufin.status == 'ARRIVE WAREHOUSE':
            print('a truck with truck_id = ' + str(ufin.truckid) + ' has arrived at warehouse, notify Amazon')
            send_amazon = True
            to_amazon.acks.append(ufin.seqnum)

            UA_TruckArrived = uapb.UA_TruckArrived()
            UA_TruckArrived.truck_id = ufin.truckid
            with whnum_lock:
                UA_TruckArrived.whnum = whnum
            with seq_lock:
                UA_TruckArrived.seqnum = seqnum
                seqnum += 1
            to_amazon.truckArrived.append(UA_TruckArrived)

            print('DB update: update truck status = "loading" for truck_id = ' + str(ufin.truckid))
            update_truck_status(Cdb, ufin.truckid, 'loading')

        if ufin.status == 'idle' or ufin.status == 'IDLE':
            print('DB update: update truck status = "idle" for truck_id = ' + str(ufin.truckid))
            update_truck_status(Cdb, ufin.truckid, 'idle')

    
    # check UDeliveryMade
    for udel in UResponse.delivered:
        print('receive UDeliveryMade from the world')
        send_amazon = True
        send_world = True
        # to_world.acks.append(udel.seqnum)

        to_world.acks.append(udel.seqnum)

        UA_Delivered = uapb.UA_Delivered()
        UA_Delivered.packageid = udel.packageid
        UA_Delivered.truckid = udel.truckid
        with seq_lock:
            UA_Delivered.seqnum = seqnum
            seqnum += 1

        to_amazon.delivered.append(UA_Delivered)
        
        
        print('DB update: update package stautus = "delivered" for package_id = ' + str(udel.packageid))
        update_package_status(Cdb, udel.packageid, 'delivered')
        print('DB update: update truck status = "idle" for truck_id = ' + str(udel.truckid))
        update_truck_status(Cdb, udel.truckid, 'idle')
        # send_email(udel.packageid)


    # check UTruck
    for utru in UResponse.truckstatus:
        print('receive Utruck from the world')
        send_world = True
        to_world.acks.append(utruck.seqnum) 
        print('DB update: update package position for truck_id = ' + str(utru.truckid) + ', new position = ' + utru.x + ',' +utru.y)   
        update_package_pos(Cdb, utru.truckid, utru.x, utru.y)


    # check UErr
    for uerr in UResponse.error:
        print('receive UErr from the world')
        print('Erro: ' + uerr.err)
        print('Original seqnum: ' + str(uerr.originseqnum))
        print('Current seqnum: ' + str(uerr.seqnum))


    if send_world:
        print('to_world:')
        print(to_world)
        send_msg(Cw, to_world)    
    if send_amazon:
        print('to_amazon:')
        print(to_amazon)
        send_msg(Ca, to_amazon)



# connection to Amazon
def connect_to_amazon(hostname, port):
    print('Connecting to Amazon...')
    # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    amazon = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    amazon.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    amazon.bind(('', port))
    amazon.listen(5)
    amazon_socket, addr = amazon.accept()

    return amazon_socket


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



def process_UA_Command(UA_Commands, Cw, Ca, Cdb):
    global seqnum
    global whnum
    to_amazon = uapb.UA_Responses()
    to_world = wupb.UCommands()
    send_amazon = False
    send_world = False

    print(UA_Commands)
    # print('recieve Amazon truck call')
    for uatru in UA_Commands.truckCall:
        send_world = True
        send_amazon = True
        # received UA_TruckCall from Amazon and send UCommands to world
        print('received UA_TruckCall from Amazon')
        to_amazon.acks.append(uatru.seqnum)
        # set up UCommands
        UGoPickup = to_world.pickups.add()
        UGoPickup.truckid = find_free_truck(Cdb)
        UGoPickup.whid = uatru.whnum
        with whnum_lock:
            whnum = uatru.whnum
        with seq_lock:
            UGoPickup.seqnum = seqnum
            seqnum += 1 

        # update the databse
        print('DB update: insert new package with package_id = ' + str(uatru.package_id) + 
            ' and owner_id = ' + str(uatru.owner_id))
        insert_package(Cdb, uatru.package_id, None, None,  'packed', UGoPickup.truckid, 
            'uatru.products.description', uatru.owner_id)
        print('DB update: update truck status = "traveling" for truck_id = ' + str(UGoPickup.truckid))
        update_truck_status(Cdb, UGoPickup.truckid, 'traveling')


    for uagod in UA_Commands.goDeliver:
        send_world = True
        print('receive UA_GoDeliver from Amazon')
        to_amazon.acks.append(uagod.seqnum)
        UGoDeliver = to_world.deliveries.add()
        UGoDeliver.truckid = uagod.truckid
        pack = UGoDeliver.packages.add()
        pack.packageid = uagod.packageid
        pack.x = uagod.x
        pack.y = uagod.y
        
        
        with seq_lock:
            UGoDeliver.seqnum = seqnum
            seqnum += 1

        print('DB update: update truck status = "delivering" for truck_id = ' + str(uagod.truckid))
        update_truck_status(Cdb, uagod.truckid, 'delivering')
        print('DB update: update package status = "delivering" for package_id = ' + str(uagod.packageid))
        update_package_status(Cdb, uagod.packageid, 'delivering')
        print('DB update: update package destination = ' + str(uagod.x) +', ' + str(uagod.y))
        update_package_dst(Cdb, uagod.packageid, uagod.x, uagod.y)


    if send_world:
        print('to_world:')
        print(to_world)
        send_msg(Cw, to_world)    
    if send_amazon:
        print('to_amazon:')
        print(to_amazon)
        send_msg(Ca, to_amazon)



def send_email(packageid):
    global smtp_server
    global email_port
    global sender_email
    global email_password
    message = 'Your package ' + str(packageid) + ' has been delivered!'
    message = 'Subject: Package Delivered\n\n' + message
    context = ssl.create_default_context()
    receiver_email = sender_email

    with smtplib.SMTP(smtp_server, email_port) as server:
        server.ehlo()  # Can be omitted
        server.starttls(context=context)
        server.ehlo()  # Can be omitted
        server.login(sender_email, email_password)
        server.sendmail(sender_email, receiver_email, message)