import socket
import random
import sys
import itertools
import hashlib
import threading
import time
import logging
import datetime


my_ip = "127.0.0.1"
if len(sys.argv) == 3:
    my_ip = sys.argv[1]
    my_peer_server_port = int(sys.argv[2])
if len(sys.argv) == 2:
    my_peer_server_port = int(sys.argv[1])
if len(sys.argv) == 1:
    print("Give atleast port number as an argument")
message_list = []
peer_list = []
all_peer_servers = []
all_peer_clients = []
threads = []
all_seed_nodes = []
lock = threading.Lock()


logfilename = "outputpeer_" + my_ip + ":" + str(my_peer_server_port) + ".txt"
logging.basicConfig(filename=logfilename, level=logging.INFO, format="%(message)s")


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
            + seed[1][0]
            + "_"
            + str(seed[1][1])
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
    logging.info("peer list obtained from seeds: {}".format(peer_list))
    print("peer list obtained from seeds: {}".format(peer_list))
    if len(peer_list) > 4:
        peer_list = random.sample(peer_list, k=4)


""" This peer connects to the other peers obtained from the connected seeds and hold the connection with each peer in different listen_from_peer_server threads """


def connect_to_peer_servers():
    for peer in peer_list:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((peer[0], int(peer[1])))
        connection = (sock, (peer[0], int(peer[1])), [0, 0, 0], [1, 1, 1])
        all_peer_servers.append(connection)
        # print("all_peer_servers",all_peer_servers)
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
    while True:
        try:
            data = (sock.recv(4096)).decode("utf-8")
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
                parts = data.split(":")
                if len(parts) == 3:
                    if str(addr[1]) == parts[1].split("_")[1]:
                        logging.debug("Original listen_from_peer_server:", addr, data)
                    else:
                        logging.debug("Forwarded listen_from_peer_server:", addr, data)
                    add_to_ml(data)
        except:
            print("error in receiving message from peer server:", addr)


""" Receives the Liveness request/reply and gossip messages from peers which acts like client to this peer"""


def listen_from_peer_client(conn, addr):
    # print("listen from PPPeer client")
    while True:
        try:
            data = (conn.recv(4096)).decode("utf-8")
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
                parts = data.split(":")
                if len(parts) == 3:
                    if str(addr[1]) == parts[1].split("_")[1]:
                        logging.debug("Original listen_from_peer_client:", addr, data)
                    else:
                        logging.debug("Forwarded listen_from_peer_client:", addr, data)
                    add_to_ml(data)
        except:
            print("error in receiving message from peer client:", addr)


""" sends liveness replies to the conn's which sended liveness request message """


def send_liveness_reply(conn, reply_message):
    try:
        conn.send(str.encode(reply_message))
        logging.debug("sending liveness reply message:", reply_message)
    except:
        print("error")


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


""" append the Gossip message to Message List"""


def add_to_ml(data):
    message = data.split(":")
    message_hash = hashlib.sha256(str.encode(data)).hexdigest()
    # print(" message hash : ", message_hash)
    lock.acquire()
    if message_hash not in message_list:
        # print("received gossip message from ", message[1], " : ", message[2], " at timestamp : ", message[0])
        logging.info("received gossip message: {},{}".format(data, gettime()))
        print("received gossip message: {},{}".format(data, gettime()))
        message_list.append(message_hash)
        send_single_message(data, "forward")
    lock.release()


def send_liveness_request_messages():
    while True:
        liveness_message = (
            "Liveness Request:"
            + gettime()
            + ":"
            + str(my_ip)
            + "_"
            + str(my_peer_server_port)
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
                    # print("entered flag == 1 if condition for all_peer_servers")
                    # print("server[2]: ",server[2])
                    # print("server[3]: ",server[3])
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
                print(" error in sending liveness message to server: ", server[1][1])

        for client in all_peer_clients:
            try:
                conn = client[0]
                flag = 0
                for i in range(3):
                    if client[3][i] == 1:
                        flag = 1
                        break
                if flag == 1:
                    # print("entered flag == 1 if condition for all_peer_clients")
                    # print("client[2]: ",client[2])
                    # print("client[3]: ",client[3])
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
                print("error in sending liveness message to client : ", client[1][1])
        time.sleep(13)


def send_gossip_messages():
    for i in range(10):
        final_message = (
            gettime()
            + ":"
            + str(my_ip)
            + "_"
            + str(my_peer_server_port)
            + ":"
            + "Message "
            + str(i)
        )
        send_single_message(final_message, "generate")
        time.sleep(5)


"""send a single message to all the connected peers """


def send_single_message(message, flag):
    if flag == "generate":
        message_hash = hashlib.sha256(str.encode(message)).hexdigest()
        message_list.append(message_hash)
        logging.debug("Generating Gossip Message : " + message)
    # if(flag == "forward"):
    # print("Forwarding Gossip Message : " + message)
    # print("all_peer_servers: ",all_peer_servers)
    # print("all_peer_clients: ",all_peer_clients)
    for server in all_peer_servers:
        try:
            s = server[0]
            s.send(str.encode(message))
            # print("server ",server[1],message)
        except:
            print(" error in sending gossip message to server: ", server[1][1])

    for client in all_peer_clients:
        try:
            conn = client[0]
            conn.send(str.encode(message))
            # print("client ",client[1],message)
        except:
            print("error in sending gossip message to client : ", client[1][1])


servers = read_seeds_from_confg()
selected_server_list = connect_to_seeds(servers)
pick_4random_peers()
logging.debug("peer_list obtained from the connected seeds: ", peer_list)
connect_to_peer_servers()
t1 = threading.Thread(target=accept_connections_from_peer_clients, daemon=True, args=())
threads.append(t1)
t1.start()
t2 = threading.Thread(target=send_gossip_messages, daemon=True, args=())
threads.append(t1)
t2.start()
t3 = threading.Thread(target=send_liveness_request_messages, daemon=True, args=())
threads.append(t3)
t3.start()
for thread in threads:
    thread.join()
