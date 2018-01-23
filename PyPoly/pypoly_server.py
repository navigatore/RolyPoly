# TODO:
# Usuwanie użytkowników


import argparse
import queue
import signal
import socket
import sys
import threading
import time

import rolypoly_pb2


class SocketInfo:
    def __init__(self, addr, port):
        self.addr = addr
        self.port = port

    @staticmethod
    def from_str(init_str):
        addr, port = init_str.split(':')
        port = int(port)
        return SocketInfo(addr, port)


class ClientInfo:
    def __init__(self, uid, name, socketinfo, last_alive):
        self.uid = uid
        self.name = name
        self.socketinfo = socketinfo
        self.last_alive = last_alive

    def __repr__(self):
        return self.name + '<id: ' + str(self.uid) + '>'


class SystemUserInfo:
    def __init__(self, uid, name, sid, last_alive):
        self.uid = uid
        self.name = name
        self.sid = sid
        self.last_alive = last_alive

    def __str__(self):
        return self.name + ' <id: ' + str(self.uid) + ' @ ' + str(self.sid) + '>'

    def __repr__(self):
        return self.__str__()


class Event:
    def __init__(self, ev_type, content=None):
        self.ev_type = ev_type
        self.content = content

    def __str__(self):
        s = 'Event type: ' + self.ev_type
        if self.content is not None:
            s += '  content: ' + str(self.content)
        return s


def millitime():
    return int(time.time() * 1000)


MSG_RESEND_TIME = 0.5
SERV_DISCOVERY_TIME = CLIENT_DISCOVERY_TIME = 5
CLIENT_MAX_ALIVE_TIME = 10
SERV_MAX_ALIVE_TIME = 20
USERS_DISCOVERY_TIME = 2
USERS_MAX_ALIVE_TIME = 8
UDP_MAXLEN = 1024


q = queue.Queue()
nbs = []
my_id = millitime()     # Own ID is time (in milliseconds) when the server started
known_servers = {my_id: millitime()}
my_port = 0
my_clients = {}
system_users = {}
rout_table = {}


def sigint_handler(_, __):
    # Nothing should be done, because the main thread is waiting for signal
    pass


########################################################################################################################
def listener(port):
    # Listener is waiting for messages from other servers
    # As soon as it gets something, it puts it into queue
    print('Listener started on port', port, '...')
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('', port))

    while True:
        data, client_addr = s.recvfrom(UDP_MAXLEN)
        msg = rolypoly_pb2.GenericMessage()
        msg.ParseFromString(data)
        if msg.type == 'EOF':
            break
        elif msg.type == 'GetKnownServers':
            q.put(Event('SEND_KNOWN_SERVERS', content=SocketInfo(client_addr[0], msg.port)))
        elif msg.type == 'GetKnownUsers':
            q.put(Event('SEND_KNOWN_USERS', content=SocketInfo(client_addr[0], msg.port)))
        elif msg.type == 'KnownServers':
            q.put(Event('MERGE_KNOWN_SERVERS', content=parse_known_servers(msg.known_servers.servers)))
        elif msg.type == 'KnownUsers':
            q.put(Event('MERGE_KNOWN_USERS', content=parse_known_users(msg.known_users.users)))
        elif msg.type == 'ConnectRequest':
            q.put(Event('ADD_NEW_CLIENT', content=parse_connect_request(*client_addr, msg.connect_request)))
        elif msg.type == 'Pong':
            q.put(Event('CLIENT_ALIVE', content=msg.u_id))
        elif msg.type == 'NewSystemUserInfo':
            q.put(Event('NEW_SYSTEM_USER', content=parse_new_system_user_info(msg.new_system_user_info)))
        elif msg.type == 'DelSystemUserInfo':
            q.put(Event('DEL_SYSTEM_USER', content=msg.u_id))
        elif msg.type == 'CountHops':
            q.put(Event('RETURN_HOPS', content=(SocketInfo(client_addr[0], msg.hops.port), msg.hops.s_id)))
        elif msg.type == 'HopsFrom':
            q.put(Event('UPDATE_HOPS', content=(SocketInfo(client_addr[0], msg.hops.port), msg.hops.s_id, msg.hops.hops)))
        elif msg.type == 'Message':
            q.put(Event('SEND_MESSAGE', content=(msg.message.sender_id, msg.message.receiver_id, msg.message.text)))
        elif msg.type == 'GetUserList':
            q.put(Event('SEND_USER_LIST', content=SocketInfo(*client_addr)))

    s.close()
    print('Listener stopped')


def parse_known_servers(ks_proto):
    ks = {}
    for i in range(len(ks_proto)):
        ks[ks_proto[i].s_id] = ks_proto[i].last_alive

    return ks


def parse_known_users(ku_proto):
    ku = {}
    for i in range(len(ku_proto)):
        suinfo = SystemUserInfo(ku_proto[i].u_id, ku_proto[i].username, ku_proto[i].s_id, ku_proto[i].last_alive)
        ku[suinfo.uid] = suinfo

    return ku


def parse_connect_request(addr, port, cr_proto):
    socketinfo = SocketInfo(addr, port)
    return ClientInfo(cr_proto.u_id, cr_proto.username, socketinfo, millitime())


def parse_new_system_user_info(info):
    return SystemUserInfo(info.u_id, info.username, info.s_id, millitime())


########################################################################################################################
def process():
    print('Processor started')

    global proc_sock
    proc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    clear_rout_table()

    server_discovery()
    client_discovery()
    user_discovery()

    remove_inactive_servers()
    remove_inactive_clients()
    remove_inactive_users()

    while True:
        ev = q.get()
        if ev is None:
            break
        # print(ev)
        if ev.ev_type == 'SERVER_DISCOVERY':
            server_discovery()
        elif ev.ev_type == 'CLIENT_DISCOVERY':
            client_discovery()
        elif ev.ev_type == 'USER_DISCOVERY':
            user_discovery()
        elif ev.ev_type == 'SEND_KNOWN_SERVERS':
            send_known_servers(ev.content)
        elif ev.ev_type == 'SEND_KNOWN_USERS':
            send_known_users(ev.content)
        elif ev.ev_type == 'MERGE_KNOWN_SERVERS':
            merge_known_servers(ev.content)
        elif ev.ev_type == 'MERGE_KNOWN_USERS':
            merge_known_users(ev.content)
        elif ev.ev_type == 'REMOVE_INACTIVE_SERVERS':
            remove_inactive_servers()
        elif ev.ev_type == 'REMOVE_INACTIVE_CLIENTS':
            remove_inactive_clients()
        elif ev.ev_type == 'REMOVE_INACTIVE_USERS':
            remove_inactive_users()
        elif ev.ev_type == 'ADD_NEW_CLIENT':
            add_new_client(ev.content)
        elif ev.ev_type == 'CLIENT_ALIVE':
            client_alive(ev.content)
        elif ev.ev_type == 'NEW_SYSTEM_USER':
            new_system_user(ev.content)
        elif ev.ev_type == 'DEL_SYSTEM_USER':
            del_system_user(ev.content)
        elif ev.ev_type == 'RETURN_HOPS':
            return_hops(*ev.content)
        elif ev.ev_type == 'UPDATE_HOPS':
            update_hops(*ev.content)
        elif ev.ev_type == 'SEND_MESSAGE':
            send_msg(*ev.content)
        elif ev.ev_type == 'SEND_USER_LIST':
            send_user_list(ev.content)
        else:
            raise NotImplementedError('This kind of event is not supported: ' + ev.ev_type)
        q.task_done()

    proc_sock.close()
    print('Processor stopped')


def put_event(event):
    q.put(event)


def server_discovery():
    for nb in nbs:
        msg = rolypoly_pb2.GenericMessage()
        msg.type = 'GetKnownServers'
        msg.port = my_port
        proc_sock.sendto(msg.SerializeToString(), (nb.addr, nb.port))
    threading.Timer(SERV_DISCOVERY_TIME, put_event, args=[Event('SERVER_DISCOVERY')]).start()


def client_discovery():
    for cid in my_clients:
        msg = rolypoly_pb2.GenericMessage()
        msg.type = 'Ping'
        proc_sock.sendto(msg.SerializeToString(), (my_clients[cid].socketinfo.addr, my_clients[cid].socketinfo.port))
    threading.Timer(CLIENT_DISCOVERY_TIME, put_event, args=[Event('CLIENT_DISCOVERY')]).start()


def user_discovery():
    for nb in nbs:
        msg = rolypoly_pb2.GenericMessage()
        msg.type = 'GetKnownUsers'
        msg.port = my_port
        proc_sock.sendto(msg.SerializeToString(), (nb.addr, nb.port))
    threading.Timer(USERS_DISCOVERY_TIME, put_event, args=[Event('USER_DISCOVERY')]).start()


def send_known_servers(receiver):
    known_servers[my_id] = millitime()  # Update my alive timestamp
    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'KnownServers'
    for i, sid in enumerate(known_servers):
        msg.known_servers.servers.add()
        msg.known_servers.servers[i].s_id = sid
        msg.known_servers.servers[i].last_alive = known_servers[sid]

    proc_sock.sendto(msg.SerializeToString(), (receiver.addr, receiver.port))


def send_known_users(receiver):
    curtime = millitime()
    for c in my_clients:    # Update my clients timestamps
        system_users[c].last_alive = curtime

    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'KnownUsers'
    for i, uid in enumerate(system_users):
        suinfo = system_users[uid]
        msg.known_users.users.add()
        msg.known_users.users[i].u_id = suinfo.uid
        msg.known_users.users[i].username = suinfo.name
        msg.known_users.users[i].s_id = suinfo.sid
        msg.known_users.users[i].last_alive = suinfo.last_alive

    proc_sock.sendto(msg.SerializeToString(), (receiver.addr, receiver.port))


def merge_known_servers(other_servers):
    for sid in other_servers:
        if sid not in known_servers:
            known_servers[sid] = other_servers[sid]
            clear_rout_table()
            print('# Server', sid, ' connected #')
        else:
            known_servers[sid] = max(known_servers[sid], other_servers[sid])


def merge_known_users(other_users):
    for uid in other_users:
        if other_users[uid].sid != my_id:
            if uid not in system_users:
                system_users[uid] = other_users[uid]
                print('# User', system_users[uid].name, '<id: ' + str(uid) + '>', 'connected #')
            else:
                system_users[uid].last_alive = max(system_users[uid].last_alive, other_users[uid].last_alive)


def remove_inactive_servers():
    known_servers[my_id] = millitime()
    curtime = millitime()
    for s in list(known_servers):
        if (curtime - known_servers[s]) > SERV_MAX_ALIVE_TIME * 1000:
            del known_servers[s]
            clear_rout_table()
            print('# Server', s, ' disconnected #')
    threading.Timer(SERV_DISCOVERY_TIME, put_event, args=[Event('REMOVE_INACTIVE_SERVERS')]).start()


def remove_inactive_clients():
    curtime = millitime()
    for cid in list(my_clients):
        if (curtime - my_clients[cid].last_alive) > CLIENT_MAX_ALIVE_TIME * 1000:
            print('# Client', system_users[cid].name, '<id: ' + str(cid) + '>', 'disconnected #')
            del my_clients[cid]
            del system_users[cid]
            msg = rolypoly_pb2.GenericMessage()
            msg.type = 'DelSystemUserInfo'
            msg.u_id = cid
            for nb in nbs:
                proc_sock.sendto(msg.SerializeToString(), (nb.addr, nb.port))
    threading.Timer(CLIENT_DISCOVERY_TIME, put_event, args=[Event('REMOVE_INACTIVE_CLIENTS')]).start()


def remove_inactive_users():
    curtime = millitime()
    for c in my_clients:
        system_users[c].last_alive = curtime

    for uid in list(system_users):
        if (curtime - system_users[uid].last_alive) > USERS_MAX_ALIVE_TIME * 1000:
            print('# User', system_users[uid].name, '<id: ' + str(uid) + '>', 'disconnected #')
            del system_users[uid]
    threading.Timer(USERS_DISCOVERY_TIME, put_event, args=[Event('REMOVE_INACTIVE_USERS')]).start()


def add_new_client(clientinfo):
    uid = clientinfo.uid
    name = clientinfo.name
    my_clients[clientinfo.uid] = clientinfo
    system_users[clientinfo.uid] = SystemUserInfo(uid, name, my_id, clientinfo.last_alive)

    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'Connected'
    proc_sock.sendto(msg.SerializeToString(), (clientinfo.socketinfo.addr, clientinfo.socketinfo.port))

    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'NewSystemUserInfo'
    msg.new_system_user_info.u_id = uid
    msg.new_system_user_info.username = name
    msg.new_system_user_info.s_id = my_id
    for nb in nbs:
        proc_sock.sendto(msg.SerializeToString(), (nb.addr, nb.port))
    print('# Client', name, '<id: ' + str(clientinfo.uid) + '> connected #')


def client_alive(cid):
    curtime = millitime()
    my_clients[cid].last_alive = curtime


def new_system_user(info):
    if info.uid not in system_users:
        system_users[info.uid] = info
        msg = rolypoly_pb2.GenericMessage()
        msg.type = 'NewSystemUserInfo'
        msg.new_system_user_info.u_id = info.uid
        msg.new_system_user_info.username = info.name
        msg.new_system_user_info.s_id = info.sid
        for nb in nbs:
            proc_sock.sendto(msg.SerializeToString(), (nb.addr, nb.port))
        print('# User', system_users[info.uid].name, '<id: ' + str(info.uid) + '>', 'connected #')


def del_system_user(uid):
    if uid in system_users:
        print('# User', system_users[uid].name, '<id: ' + str(uid) + '> disconnected #')
        del system_users[uid]
        msg = rolypoly_pb2.GenericMessage()
        msg.type = 'DelSystemUserInfo'
        msg.u_id = uid
        for nb in nbs:
            proc_sock.sendto(msg.SerializeToString(), (nb.addr, nb.port))


def send_msg(sender_id, receiver_id, text):
    if receiver_id in system_users:
        if receiver_id in my_clients:
            msg = rolypoly_pb2.GenericMessage()
            msg.type = 'Message'
            msg.message.sender_id = sender_id
            msg.message.receiver_id = receiver_id
            msg.message.text = text
            si = my_clients[receiver_id].socketinfo
            proc_sock.sendto(msg.SerializeToString(), (si.addr, si.port))
        elif system_users[receiver_id].sid not in rout_table:
            find_route(system_users[receiver_id].sid)
            threading.Timer(MSG_RESEND_TIME, put_event, args=[Event('SEND_MESSAGE', content=(sender_id, receiver_id, text))]).start()
        else:
            msg = rolypoly_pb2.GenericMessage()
            msg.type = 'Message'
            msg.message.sender_id = sender_id
            msg.message.receiver_id = receiver_id
            msg.message.text = text
            si = rout_table[system_users[receiver_id].sid][1]
            proc_sock.sendto(msg.SerializeToString(), (si.addr, si.port))


def find_route(sid):
    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'CountHops'
    msg.hops.s_id = sid
    msg.hops.port = my_port
    for nb in nbs:
        proc_sock.sendto(msg.SerializeToString(), (nb.addr, nb.port))


def return_hops(socketinfo, sid):
    if sid in rout_table:   # I looked for route to this sid already
        if rout_table[sid] is not None:     # I know working route
            msg = rolypoly_pb2.GenericMessage()
            msg.type = 'HopsFrom'
            msg.hops.s_id = sid
            msg.hops.hops = rout_table[sid][0]
            msg.hops.port = my_port
            proc_sock.sendto(msg.SerializeToString(), (socketinfo.addr, socketinfo.port))

    else:
        rout_table[sid] = None  # Marking as None means "I don't know the route yet, but started looking for it"
        find_route(sid)


def update_hops(socketinfo, sid, hops):
    if sid not in rout_table or rout_table[sid] is None or rout_table[sid][0] > hops + 1:
        rout_table[sid] = (hops + 1, socketinfo)
        for nb in nbs:
            msg = rolypoly_pb2.GenericMessage()
            msg.type = 'HopsFrom'
            msg.hops.s_id = sid
            msg.hops.hops = rout_table[sid][0]
            msg.hops.port = my_port
            proc_sock.sendto(msg.SerializeToString(), (nb.addr, nb.port))


def clear_rout_table():
    global rout_table
    rout_table = {my_id: (0, SocketInfo('127.0.0.1', my_port))}


def send_user_list(socketinfo):
    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'UserList'
    for i, uid in enumerate(system_users):
        u = system_users[uid]
        msg.userlist.users.add()
        msg.userlist.users[i].s_id = uid
        msg.userlist.users[i].username = u.name
    proc_sock.sendto(msg.SerializeToString(), (socketinfo.addr, socketinfo.port))


########################################################################################################################
def load_config(config_filename):
    with open(config_filename) as f:
        for line in f:
            ln = line.rstrip()
            if ln != '':
                nbs.append(SocketInfo.from_str(ln))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=int)
    parser.add_argument('config')
    args = parser.parse_args()

    load_config(args.config)

    print('# PyPoly server started #')
    print('# ID:', my_id, '#')

    global my_port
    my_port = args.port

    threads = []
    pt = threading.Thread(target=process)
    pt.start()
    threads.append(pt)
    lt = threading.Thread(target=listener, args=[args.port])
    lt.start()
    threads.append(lt)

    signal.signal(signal.SIGINT, sigint_handler)
    signal.pause()  # Wait for SIGINT
    print('\nSIGINT detected, shutting down')
    q.put(None)

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)    # UDP socket
    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'EOF'
    s.sendto(msg.SerializeToString(), ('127.0.0.1', my_port))
    s.close()

    for t in threads:
        t.join()

    return 0


if __name__ == '__main__':
    sys.exit(main())
