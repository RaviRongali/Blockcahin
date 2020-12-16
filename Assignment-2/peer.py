import socket
import random
import sys,os
import itertools
import hashlib
import threading
import time
import logging
import datetime
import json
import random
import numpy as np
# import Filelock
my_ip = "127.0.0.1"
inter_arrival_time = 2
if len(sys.argv) == 4:
    inter_arrival_time = int(sys.argv[1])
    my_ip = sys.argv[2]
    my_peer_server_port = int(sys.argv[3])
if len(sys.argv) == 3:
    inter_arrival_time = int(sys.argv[1])
    my_peer_server_port = int(sys.argv[2])
if len(sys.argv) == 2:
    my_peer_server_port = int(sys.argv[1])
if len(sys.argv) == 1:
    sys.exit("Give atleast port number as an argument")
message_list = []
peer_list = []
all_peer_servers = []
all_peer_clients = []
threads = []
all_seed_nodes = []
lock = threading.Lock()
connlock=threading.Lock()
jsonfile = "outputpeer_honest_" + my_ip + ":" + str(my_peer_server_port) + ".json"
queue =[]
init_queue=[]
nodeHashPower=10
newBlock_received = False
logfilename = "outputpeer_" + my_ip + ":" + str(my_peer_server_port) + ".txt"
try:
    os.remove(logfilename)
except:
    pass
logging.basicConfig(filename=logfilename, level=logging.INFO, format="%(asctime)s %(message)s",datefmt='%Y-%m-%d %H:%M:%S')
no_of_blocks = -1
init_received = 0
filelock=threading.Lock()
#####  Define block structure
class Block:
    def __init__(self, previousHash, time_stamp, merkel_root,index = -1):
        self.index = index
        self.previousHash = str(previousHash)
        self.time_stamp = time_stamp
        self.merkel_root = merkel_root
        self.hash = calculateHash(self)


def calculateHash(block):
    value=str(block.previousHash)+str(block.merkel_root)+str(block.time_stamp)
    hashVal = hashlib.sha256(str.encode(value)).hexdigest()
    return "0x"+str(hashVal[-4:])


def createJson():
    dictionary ={ 
        "0x9e1c" : { 
        "index" : 1, 
        "previousHash" : "null", 
        "merkel_root" : "0x0000",
        "time_stamp" : "0",
        "type":"other"
    }
    }
    json_object = json.dumps(dictionary, indent = 4) 
    with open(jsonfile, "w") as outfile: 
        outfile.write(json_object) 

def write_json(data, filename=jsonfile): 
    filelock.acquire()
    with open(filename,'w') as f: 
        json.dump(data, f, indent=4) 
    filelock.release()
      
def add_newBlock(CurBlock,me=0):
    filelock.acquire()
    with open(jsonfile) as json_file: 
        data = json.load(json_file) 
        # python object to be appended 
        curHash = CurBlock.hash
        chara=""
        if me==0:
            chara="other"
        else:
            chara="me"
        data[curHash]={ 
        "index" : CurBlock.index, 
        "previousHash" : CurBlock.previousHash, 
        "merkel_root" : CurBlock.merkel_root,
        "time_stamp" : CurBlock.time_stamp,
        "type":chara
    }
        # appending data to emp_details  
    filelock.release()  
    write_json(data) 

def getNumberofblocks():
    filelock.acquire()
    with open(jsonfile) as json_file: 
        data = json.load(json_file)
        data=data.items()
        # hash_keys=data.keys()
        maxIndex=0
        for i,j in data:
            maxIndex=max(maxIndex,j["index"])
        filelock.release()
        return maxIndex
    

def getblock(hash):
    filelock.acquire()
    with open(jsonfile) as json_file: 
        data = json.load(json_file) 
        prev_hash=data[hash]["previousHash"]
        timestamp=data[hash]["time_stamp"]
        merkelroot=data[hash]["merkel_root"]
        filelock.release()
        return Block(prev_hash,timestamp,merkelroot)
    

###### Block Mining

def getLatestBlock():
    filelock.acquire()
    with open(jsonfile) as json_file: 
        data = json.load(json_file) 
        data=data.items()
        ind = -1
        hash_key = 0
        timestamp = 2 * time.time()
        for x,v in data:
            if v["index"] > ind:
                ind = v["index"]
                timestamp = v["time_stamp"]
                hash_key = x
            elif v["index"] == ind:
                if timestamp > v["time_stamp"]:
                    timestamp = v["time_stamp"]
                    hash_key = x
        filelock.release()
        return hash_key,ind

def isValidNewBlock(prev_hash,merkel_root,time_stamp):
    value=str(prev_hash)+str(merkel_root)+str(time_stamp)
    hashVal = hashlib.sha256(str.encode(value)).hexdigest()
    hashVal = "0x"+str(hashVal[-4:])
    filelock.acquire()
    with open(jsonfile) as json_file: 
        data = json.load(json_file) 
        hash_keys=data.keys()
        filelock.release()
        if prev_hash not in hash_keys or hashVal in hash_keys:
            return -1
            # print("invalid_block_prevHashNotFound")
        t = time.time()
        try:  
            if abs(t - float(time_stamp)) > 3600000:
                return -1
        except:
            return -1
            # print("invalid_block_timeStampError")
        return data[prev_hash]["index"]

def generateNextBlock():
    previousBlock,prev_ind = getLatestBlock()
    nextIndex = prev_ind + 1
    nextTimestamp = time.time()
    # nextHash = calculateHash(nextIndex, previousBlock.hash, nextTimestamp,"0x0000")
    return Block(previousBlock, str(nextTimestamp), "0x0000",nextIndex)


def add_to_queue(newBlock):
    if newBlock not in queue:
        queue.append(newBlock)

def process_queue():
    while True:
        if len(queue)>0:
            cur_block = queue.pop(0)
            a = isValidNewBlock(cur_block.previousHash,cur_block.merkel_root,cur_block.time_stamp)
            if a != -1:
                cur_block.index = a+1
                add_newBlock(cur_block)
                broadcast_block(cur_block,"generate")
                logging.info("validated the received block with hash {} and broadcasting".format(cur_block.hash))

############# Assignment-1 functions
def gettime():
    x = str(datetime.datetime.now())
    parts = x.split(":")
    y = parts[0] + "_" + parts[1] + "_" + parts[2]
    return y


"""It reads the seed's  ip and port numbers and returns the list of those"""


def read_seeds_from_confg():
    fh = open("config.txt")
    lines = fh.readlines()
    servers = []
    for line in lines:
        ip = line.split(":")[0]
        port = line.split(":")[1]
        logging.debug("ip= ", ip, "port= ", port)
        servers.append([ip, port])
    fh.close()
    return servers


"""It will start making connections with the [n/2] + 1 seeds and hold those connections with different threads"""


def connect_to_seeds(servers):
    t = int(len(servers) / 2)
    # no_of_conn = t+1+int((t-1)*random.random())
    no_of_conn = t + 1
    selected_server_list = random.sample(servers, k=no_of_conn)
    for server in selected_server_list:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server[0], int(server[1])))
        data = sock.recv(4096)
        logging.debug(data.decode("utf-8"))  # printing the  welcome messsage
        sock.send(
            str.encode(str(my_peer_server_port))
        )  # sending the peer server port to seed
        peer_string_list = (sock.recv(4096)).decode("utf-8")
        peers = peer_string_list.split()
        for peer in peers:
            parts = peer.split("-")
            peer_list.append([parts[1], parts[2]])
        # print(peer_string_list)
        all_seed_nodes.append((sock, (server[0], int(server[1]))))
        # sock.close()
        t = threading.Thread(target=listen_from_seed_node, daemon=True, args=(sock,))
        threads.append(t)
        t.start()
    return selected_server_list


""" Here there is nothing listen from seed node as per our requirement"""


def listen_from_seed_node(sock):
    while True:
        a = 2  # there is nothing to send to seed node


""" This peer reports the deadnode info to all connected seeds """


def send_to_all_seed_nodes(dead_node):
    for seed in all_seed_nodes:
        sock = seed[0]
        message = (
            "Dead Node:"
            + dead_node[0]
            + "_"
            + str(dead_node[1])
            + ":"
            + gettime()
            + ":"
            + my_ip
            + "_"
            + str(my_peer_server_port)
        )
        sock.send(str.encode(message))
    logging.info("Reporting Dead node info: {}".format(message))
    print("Reporting Dead node info: {}".format(message))


""" This func takes the union of all the peer lists that it received from different seeds and randomly selects a maximum of 4 distinct peer nodes"""


def pick_4random_peers():
    global peer_list
    peer_list.sort()
    peer_list = list(peer_list for peer_list, _ in itertools.groupby(peer_list))
    peer_list.remove([my_ip, str(my_peer_server_port)])
    if len(peer_list) > 4:
        print("if is called")
        peer_list = random.sample(peer_list, k=4)
    logging.info("peer list obtained from seeds: {}".format(peer_list))
    print("peer list obtained from seeds: {}".format(peer_list))


""" This peer connects to the other peers obtained from the connected seeds and hold the connection with each peer in different listen_from_peer_server threads """


def connect_to_peer_servers():
    for peer in peer_list:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((peer[0], int(peer[1])))
        connection = (sock, (peer[0], int(peer[1])), [0, 0, 0], [1, 1, 1])
        all_peer_servers.append(connection)
        welcome_message = sock.recv(4096).decode(
            "utf-8"
        )  # recieving welcome message from connection request accepted peer server
        logging.debug(welcome_message)

        # start  a thread to exchange messages
        sock.send(
            str.encode(str(my_peer_server_port))
        )  # sending my_peer_server_port to the connected peer, so that he can send message to this port

        t = threading.Thread(
            target=listen_from_peer_server,
            daemon=True,
            args=(sock, (peer[0], int(peer[1]))),
        )
        threads.append(t)
        t.start()
        # print("connect to peer servers")


""" This func accepts the connection requests made by other peers and hold the connection with each peer in different listen_from_peer_client threads"""


def accept_connections_from_peer_clients():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((my_ip, my_peer_server_port))
    except:
        print("Not able to bind the port! try with different port number")
    sock.listen(5)
    while True:
        conn, addr = sock.accept()
        welcome_message = (
            "Congratulations! You have connected to the peer node at: "
            + my_ip
            + ":"
            + str(my_peer_server_port)
            + "\n"
        )
        conn.send(
            str.encode(welcome_message)
        )  # sends the welcome message to the peer client
        # listen to messages

        client_peer_port = str(
            conn.recv(20480), "utf-8"
        )  # recieving peer client's peer server port
        connection = (conn, (addr[0], int(client_peer_port)), [0, 0, 0], [1, 1, 1])
        all_peer_clients.append(connection)
        # logging.debug("all_peer_clients",all_peer_clients)
        t = threading.Thread(
            target=listen_from_peer_client,
            daemon=True,
            args=(conn, (addr[0], int(client_peer_port))),
        )
        threads.append(t)
        t.start()
        # print("accept_connections_from_peer_clients")


""" Receives the Liveness request/reply and gossip messages from peers which acts like server to this peer"""


def listen_from_peer_server(sock, addr):
    # print("listen from PPPPeer server")
    global no_of_blocks,init_received,newBlock_received
    prev_hash = ""
    i =1
    while True:
        try:
            # data = (sock.recv(4096)).decode("utf-8")
            data1 = (sock.recv(4096)).decode("utf-8")
            data1=data1.split("***")
            if data1[-1]=="":
                data1.pop()
            while(len(data1)>0):
                data=data1.pop(0)
                # print("listen from peer server:"+data)
                if data[:8] == "Liveness":
                    # print("entered Liveness if condition in listen_from_peer_server: " + data)
                    if data[9:16] == "Request":
                        # a = 2
                        message = data.split(":")
                        reply_message = (
                            "Liveness Reply:"
                            + message[1]
                            + ":"
                            + message[2]
                            + ":"
                            + str(my_ip)
                            + "_"
                            + str(my_peer_server_port)
                        )
                        t = threading.Thread(
                            target=send_liveness_reply,
                            daemon=True,
                            args=(
                                sock,
                                reply_message,
                            ),
                        )
                        threads.append(t)
                        t.start()
                    if data[9:14] == "Reply":
                        # b = 3
                        logging.debug("Recieving liveness reply message", data)
                        handle_reply_from_peer_server(data)

                else:
                    flag = data[0:2]
                    if flag=="00" :    ### receiving a new block mined by others and broadcasting to others
                        # print("351",data)
                        logging.info("Received a new block from {} added to queue".format(addr))
                        newBlock_received =  True
                        prev_hash = data[2:8]
                        merkel_root = data[8:14]
                        time_stamp = data[14:]
                        rec_block = Block(prev_hash,time_stamp,merkel_root)
                        add_to_queue(rec_block)           
                    if flag == "01":
                        logging.info("Received reply for longchain request from {}".format(addr))   ### receiving no_of_blocks and requesting index 1 block
                        data_parts = data.split(":")
                        no_of_blocks = int(data_parts[1])
                        message = str("10")+":"+data_parts[2]
                        t1i = threading.Thread(target=reply, daemon=True,args=(sock, message,),)
                        threads.append(t1i)
                        t1i.start()
                        logging.info("Requesting {} for last block with hash {} ".format(addr, data_parts[2]))
                        # print("396 asked blocks to server")
                    if flag == "10":    ### receiving requested index block and requesting next index block
                        # extract block from data
                        # print(411,"received reply for ith  blocks request",data)
                        logging.info("Received the block for request from {}".format(addr))
                        init_received+=1
                        newBlock_prevhash = data[2:8]
                        newBlock_MR = data[8:14]
                        newBlock_TS = data[14:]
                        newBlock = Block(newBlock_prevhash,newBlock_TS,newBlock_MR,no_of_blocks)
                        no_of_blocks -= 1
                        # add_to_new_queue(newBlock)
                        init_queue.append(newBlock)
                        message = str("10")+":"+newBlock_prevhash
                        if no_of_blocks > 0:
                            logging.info("Requesting {} for  block with hash {} ".format(addr,newBlock_prevhash))
                            t2k = threading.Thread(target=reply, daemon=True,args=(sock, message,),)
                            threads.append(t2k)
                            t2k.start()
                        else:
                            logging.info("Received the chain from {} and started processing the pending queue".format(addr))
                            t3i = threading.Thread(target=process_init_queue_mining, daemon=True, args=())
                            threads.append(t3i)
                            t3i.start()
        except:
            continue
            print("unable in receiving message from peer server:{} and received data is {}".format(addr,data))
            # return(0)


""" Receives the Liveness request/reply and gossip messages from peers which acts like client to this peer"""


def listen_from_peer_client(conn, addr):
    # print("listen from PPPeer client")
    global newBlock_received
    while True:
        try:
            data1 = (conn.recv(4096)).decode("utf-8")
            data1=data1.split("***")
            if data1[-1]=="":
                data1.pop()
            while(len(data1)>0):
                data=data1.pop(0)
                # print("listen from peer client:"+data)
                if data[:8] == "Liveness":
                    # print("entered Liveness if condition in listen_from_peer_client: " + data)
                    if data[9:16] == "Request":
                        # a = 2                # send_liveness_reply_to only sending node
                        message = data.split(":")
                        reply_message = (
                            "Liveness Reply:"
                            + message[1]
                            + ":"
                            + message[2]
                            + ":"
                            + str(my_ip)
                            + "_"
                            + str(my_peer_server_port)
                        )
                        t = threading.Thread(
                            target=send_liveness_reply,
                            daemon=True,
                            args=(
                                conn,
                                reply_message,
                            ),
                        )
                        threads.append(t)
                        t.start()
                    if data[9:14] == "Reply":
                        logging.debug("Recieving liveness reply message", data)
                        handle_reply_from_peer_client(data)
                        # b = 3               # update the status of connected peers and send the dead node to seeds if we don't get reply for three times

                else:
                    flag = data[0:2]
                    if flag=="00" :  ### receiving a new block mined by others and broadcasting to others
                        newBlock_received =  True
                        prev_hash = data[2:8]
                        merkel_root = data[8:14]
                        time_stamp = data[14:]
                        rec_block = Block(prev_hash,time_stamp,merkel_root)
                        add_to_queue(rec_block)
                        logging.info("Received a new block from {} added to queue".format(addr))
                    if flag == "01":
                        # print(475 ,"requesting blocks",data)    #### receiving "no of blocks" request and replying the current no of blocks in local chain
                        if data[2:] == "no_of_blocks":
                            # k=getNumberofblocks()
                            logging.info("{} is requesting longest block chain".format(addr))
                            latest_hash,latest_ind = getLatestBlock()
                            reply_message= "01"+":"+str(latest_ind)+":"+latest_hash
                            t1r = threading.Thread(target=reply, daemon=True,args=(conn, reply_message,),)
                            threads.append(t1r)
                            t1r.start()
                            logging.info("sending last block with hash {} and total no of blocks to {}".format(latest_hash,addr))
                    if flag == "10":   
                        # print(482 ,"requesting ith blocks",data)    ### receiving request of ith block in chain and replying ith block details
                        data_parts = data.split(":")
                        # print(486)
                        prev_hash= data_parts[1]
                        logging.info("received a request for block with hash {} from {}".format(prev_hash,addr))
                        # print(492)
                        block=getblock(prev_hash)
                        # print(494)
                        reply_message= "10"+str(block.previousHash)+str(block.merkel_root)+str(block.time_stamp)
                        # print(496)
                        t2r = threading.Thread(target=reply, daemon=True,args=(conn, reply_message,),)
                        threads.append(t2r)
                        t2r.start()
                        logging.info("sending the requested block with hash {} to {}".format(prev_hash,addr))
        except:
            # continue
            print("unable in receiving message from peer client:{} and received data is {}".format(addr,data))
            # return(0)


def reply(conn, reply_message):
    try:
        # connlock.acquire()
        # print("replying 503",reply_message)
        reply_message = reply_message + "***"
        conn.send(str.encode(reply_message))
        # print("sending reply message:", reply_message)
        # connlock.release()
    except:
        print("error510")




""" sends liveness replies to the conn's which sended liveness request message """

def send_liveness_reply(conn, reply_message):
    try:
        # connlock.aquire()
        reply_message = reply_message + "***"
        conn.send(str.encode(reply_message))
        # print("sending liveness reply message:", reply_message)
        # connlock.release()
    except:
        print("unable to send liveness reply")


""" update the last three states of replies from the peer clients"""


def handle_reply_from_peer_client(data):
    message = data.split(":")
    time_stamp = message[1]
    ip_port = message[3]
    parts = ip_port.split("_")
    replied_ip = parts[0]
    replied_port = parts[1]
    # print("handle_reply_from_peer_client: ",data)
    for client in all_peer_clients:
        if client[1][0] == replied_ip and client[1][1] == int(replied_port):
            for i in range(3):
                if client[2][i] == time_stamp:
                    client[3][i] = 1
                    break
            break


""" update the last three states of replies from the peer servers"""


def handle_reply_from_peer_server(data):
    message = data.split(":")
    time_stamp = message[1]
    ip_port = message[3]
    parts = ip_port.split("_")
    replied_ip = parts[0]
    replied_port = parts[1]
    # print("handle_reply_from_peer_server: ",data)
    for server in all_peer_servers:
        if server[1][0] == replied_ip and server[1][1] == int(replied_port):
            for i in range(3):
                if server[2][i] == time_stamp:
                    server[3][i] = 1
                    break
            break




def send_liveness_request_messages():
    while True:
        liveness_message = (
            "Liveness Request:"
            + gettime()
            + ":"
            + str(my_ip)
            + "_"
            + str(my_peer_server_port)
            + "***"
        )
        for server in all_peer_servers:
            try:
                s = server[0]
                flag = 0
                for i in range(3):
                    if server[3][i] == 1:
                        flag = 1
                        break
                if flag == 1:
                    server[2].pop(0)
                    server[3].pop(0)
                    server[2].append(liveness_message.split(":")[1])
                    server[3].append(0)
                    # print("entered flag == 1 if condition for all_peer_servers",s)
                    s.send(str.encode(liveness_message))
                    logging.debug(
                        "sending liveness request message: ",
                        liveness_message,
                        " to:",
                        server[1],
                    )
                else:
                    a = 2  # report to all connected seeds
                    send_to_all_seed_nodes(server[1])
                    all_peer_servers.remove(server)
            except:
                print(" unable to send liveness message to server: ", server[1][1])

        for client in all_peer_clients:
            try:
                conn = client[0]
                flag = 0
                for i in range(3):
                    if client[3][i] == 1:
                        flag = 1
                        break
                if flag == 1:
                    client[2].pop(0)
                    client[3].pop(0)
                    client[2].append(liveness_message.split(":")[1])
                    client[3].append(0)
                    # print("entered flag == 1 if condition for all_peer_clients",conn)
                    conn.send(str.encode(liveness_message))
                    logging.debug(
                        "sending liveness request message: ",
                        liveness_message,
                        " to:",
                        client[1],
                    )
                else:
                    a = 2  # report to all connected seeds
                    send_to_all_seed_nodes(client[1])
                    all_peer_clients.remove(client)

            except:
                print("unable to send liveness message to client : ", client[1][1])
        time.sleep(13)


def request_initial_blocks():
    global no_of_blocks
    if len(all_peer_servers) > 0:
        peer_server = all_peer_servers[0]
        s = peer_server[0]
        message = str("01")+str("no_of_blocks")+"***"
        s.send(str.encode(message))
        logging.info("Requesting Initial blocks from {}".format(peer_server[1]))
    else:
        # print("receive_intial_blocks -- else statement")
        no_of_blocks =0
        t2t = threading.Thread(target=process_init_queue_mining, daemon=True, args=())
        threads.append(t2t)
        t2t.start()

    



def process_init_queue_mining():
    while len(init_queue)>0:
        cur_block = init_queue.pop()
        a = isValidNewBlock(cur_block.previousHash,cur_block.merkel_root,cur_block.time_stamp)
        if a != -1:
            cur_block.index = a+1
            add_newBlock(cur_block)
            # broadcast_block(cur_block,"generate")
    t0 = threading.Thread(target=process_queue, daemon=True, args=())
    threads.append(t0)
    t0.start()
    global newBlock_received,no_of_blocks,init_received
    while True:
        global_lambda = 1/inter_arrival_time
        t = nodeHashPower * global_lambda/100
        # waiting_time = time.Duration(random.ExpFloat64()/t)
        waiting_time = np.random.exponential()/t
        # print("waiting time: ",waiting_time)
        logging.info("Started mining for new block with waiting time {}".format(waiting_time))
        start_time = time.time()
        end_time = start_time + waiting_time
        while newBlock_received == False:
            # add_to_queue(received_block),process_queue()  write this func in listen from peer server and listen from peer client funcs
            if (time.time()>end_time):
                # print("time up")
                break
            # time.sleep(0.1)
            # time.sleep(waiting_time)

        if newBlock_received == True:
            newBlock_received = False
            logging.info("Stopped mining as received a new block from others")
            # print("new block received")
            continue    
        newBlock = generateNextBlock()
        logging.info("Mining completed and broadcasting mined block with hash {}".format(newBlock.hash))
        # print("new block mined 619   ",newBlock.hash)
        add_newBlock(newBlock,1)
        broadcast_block(newBlock,"generate")
    
def broadcast_block(newBlock,flag):
    message = "00" + str(newBlock.previousHash)+ str(newBlock.merkel_root)+str(newBlock.time_stamp)+"***"
    if flag == "generate":
        message_hash = hashlib.sha256(str.encode(message)).hexdigest()
        message_list.append(message_hash)
        # logging.debug("Generating New Block : " + message)
 
        for server in all_peer_servers:
            try:
                s = server[0]
                s.send(str.encode(message))
                # print("server ",server[1],message)
            except:
                print("unable in sending gossip message to server: ", server[1][1])

        for client in all_peer_clients:
            try:
                conn = client[0]
                conn.send(str.encode(message))
                # print("client ",client[1],message)
            except:
                print("unable in sending gossip message to client : ", client[1][1])
        logging.info("Broadcasting a block with hash {} to all the peers".format(newBlock.hash))
        # print("broadcasting block 648")


createJson()
servers = read_seeds_from_confg()
selected_server_list = connect_to_seeds(servers)
pick_4random_peers()
logging.debug("peer_list obtained from the connected seeds: ", peer_list)
connect_to_peer_servers()
time.sleep(0.1)
request_initial_blocks()
t1m = threading.Thread(target=accept_connections_from_peer_clients, daemon=True, args=())
threads.append(t1m)
t1m.start()


t3 = threading.Thread(target=send_liveness_request_messages, daemon=True, args=())
threads.append(t3)
t3.start()


# t4 = threading.Thread(target=flooding_attack, daemon=True, args=())
# threads.append(t4)
# t4.start()


for thread in threads:
    thread.join()
