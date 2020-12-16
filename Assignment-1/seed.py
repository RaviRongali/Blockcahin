import socket
import select
import threading
import sys
import logging

ip = "127.0.0.1"

if len(sys.argv) == 3:
    ip = sys.argv[1]
    port = int(sys.argv[2])
if len(sys.argv) == 2:
    port = int(sys.argv[1])
if len(sys.argv) == 1:
    print("Give atleast port number as an argument")


threads = []
peer_list = []


logfilename = "outputseed_" + ip + ":" + str(port) + ".txt"
logging.basicConfig(filename=logfilename, level=logging.INFO, format="%(message)s")


def connect_to_peers():
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        serv.bind((ip, port))
        serv.listen(5)
    except:
        print("Not able to bind this port")
        return

    while True:
        conn, addr = serv.accept()
        welcome_message = (
            "Congratulations! You have connected to the seed node at: "
            + ip
            + ":"
            + str(port)
            + "\n"
        )
        conn.send(
            str.encode(welcome_message)
        )  # sends the welcome message to the peer node
        data = conn.recv(4096)  # recieves the peer server port
        peer_server_port = data.decode("utf-8")
        peer_list.append([addr[0], peer_server_port])
        # print("peer_list: ", peer_list)
        logging.info(
            "Received the connection request from: {}:{}".format(
                addr[0], str(peer_server_port)
            )
        )
        print(
            "Received the connection request from: {}:{}".format(
                addr[0], str(peer_server_port)
            )
        )
        peer_string_list = ""
        for i in range(len(peer_list)):
            peer_string_list += (
                str(i) + "-" + str(peer_list[i][0]) + "-" + str(peer_list[i][1]) + "\n"
            )
        conn.send(str.encode(peer_string_list))
        # conn.close()
        t = threading.Thread(target=listen_from_peer, daemon=True, args=(conn,))
        threads.append(t)
        t.start()
        # print("client disconnected")


def listen_from_peer(conn):
    while True:
        data = conn.recv(4096)
        message = data.decode("utf-8")
        if message == "":
            continue
        print("Dead node message recieved:" + message)
        logging.info("Dead node message recieved: {}".format(message))
        parts = message.split(":")
        dead_node = parts[1].split("_")
        dead_node_ip = dead_node[0]
        dead_node_port = dead_node[1]
        # print(dead_node_ip, dead_node_port)
        if [dead_node_ip, dead_node_port] in peer_list:
            peer_list.remove([dead_node_ip, dead_node_port])
        # print("updated_peer_list:", peer_list)


connect_to_peers()

for thread in threads:
    thread.join()

