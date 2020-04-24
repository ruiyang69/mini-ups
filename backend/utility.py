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
email_password ='Impatrick1008!'

# thread locks
world_lock = threading.Lock()
amazon_lock = threading.Lock()
seq_lock = threading.Lock()
seqnum = 0


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
    send_world = False
    send_amazon = False

    to_world = wupb.UCommands()
    to_amazon = uapb.UA_Responses()

    print('checking UFinished')
    for ufin in UResponse.completions:
        send_world = True
        to_world.acks.append(ufin.seqnum)

        if ufin.status == 'arrive warehouse':
            print('a truck with truck_id = ' + str(ufin.truckid) + ' has arrived at warehouse, notify Amazon')
            send_amazon = True
            to_amazon.acks.append(ufin.seqnum)
            UA_TruckArrived = to_amazon.truckArrived
            UA_TruckArrived.truckid = ufin.truckid
            UA_TruckArrived.whnum = get_truck_whnum(Cdb, ufin.truckid)
            with seq_lock:
                UA_TruckArrived.seqnum = seqnum
                seqnum += 1
            
            update_truck_status(Cdb, ufin.truckid, 'loading')

        if ufin.status == 'idle':
            print('DB update: update truck status = "idle" for truck_id = ' + str(ufin.truckid))
            update_truck_status(Cdb, ufin.truckid, 'idle')

    
    # check UDeliveryMade
    for udel in UResponse.delivered:
        print('receive UDeliveryMade from the world')
        send_amazon = True
        send_world = True
        # to_world.acks.append(udel.seqnum)

        to_world.ack.append(udel.seqnum)
        UA_Delivered = to_amazon.delivered
        UA_Delivered.packageid = udel.packageid
        UA_Delivered.truckid = udel.truckid
        with seq_lock:
            UA_Delivered.seqnum = seqnum
            seqnum += 1
        
        print('DB update: update package stautus = "delivered" for package_id = ' + str(udel.packageid))
        update_package_status(Cdb, udel.packageid, 'delivered')
        print('DB update: update truck status = "idle" for truck_id = ' + str(udel.truckid))
        update_truck_status(Cdb, udel.truckid, 'idle')


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
        send_msg(Cw, to_world)    
    if send_amazon:
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
    to_amazon = uapb.UA_Responses()
    to_world = wupb.UCommands()
    send_amazon = False
    send_world = False


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
        with seq_lock:
                UGoPickup.seqnum = seqnum
                seqnum += 1 

        # update the databse
        print('DB update: insert new package with package_id = ' + str(uatru.package_id))
        insert_package(Cdb, uatru.package_id, uatru.dest_x, uatru.dest_y, 'packed', UGoPickup.truckid, 
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


    if send_world:
        send_msg(Cw, to_world)    
    if send_amazon:
        send_msg(Ca, to_amazon)


    # #recieve Ufinished
    # U_response = world_ups_recv(Cw, True)
    # to_amazon_truckarr = UA_Commands
    # for uresp in U_response.completions:
    #     to_amazon_truckarr.truckArrived.append(to_amazon_truckarr.whnum, uresp.truckid, uresp.seqnum)
    #     # update truck status
    #     update_truck_status(C, uresp.truckid, "the truck is used")

    #     with seq_lock:
    #             uresp.seqnum = seqnum
    #             seqnum += 1 

    # # send truck arrived
    # send_msg(Ca, to_amazon_truckarr)

    # #recieve ack from amazon
    # a_recv = amazon_ups_recv(Ca)

    # # recieve aloaded
    # ua_rep = amazon_ups_recv(Ca)
    # to_amazon_ack2 = ua_rep.acks
    # # go deliver
    # to_world2 = wupb.UCommands()
    # for aload in ua_rep.goDeliver:
    #     to_amazon_ack2.append(aload.seqnum)                
    #     UDeliveryLocation = to_world2.deliveries
    #     UDeliveryLocation.packageid = aload.packageid
    #     UDeliveryLocation.x = aload.x
    #     UDeliveryLocation.y = aload.y
    #     to_world2.deliveries.append(aload.truckid, UDeliveryLocation, aload.seqnum)

    #     # update truck status
    #     update_truck_status(C, aload.truckid, 'idle')

    #     with seq_lock:
    #             aload.seqnum = seqnum
    #             seqnum += 1 

    # # send ack and go deliever
    # send_msg(Ca, to_amazon_ack2)
    # send_msg(Cw, to_world2)

    # #recieve Udelievered made
    # U_response_D = world_ups_recv(Cw, True)
    # to_amazon_UD = UA_Commands
    # for ud in U_response_D.delivered:
    #    to_amazon_UD.goDeliver.append(ud.truckid, ud.packageid, U_response_D.completions.x, U_response_D.completions.y, ud.seqnum)

    #    with seq_lock:
    #             ud.seqnum = seqnum
    #             seqnum += 1

    # # send delievered
    # send_msg(Ca, to_amazon_UD)

    # #recieve ack from amazon
    # b_recv = amazon_ups_recv(Ca)
