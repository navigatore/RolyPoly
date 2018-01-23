import time
import tkinter
import socket

import rolypoly_pb2


def millitime():
    return int(time.time() * 1000)


UDP_MAXLEN = 1024


my_id = millitime()
my_name = 'Anonym' + str(my_id)
receiver_id = 0
sock = s_addr = s_port = None
users_list = {}


window = tkinter.Tk()

messages = tkinter.Text(window)
messages.pack()

input_user = tkinter.StringVar()
input_field = tkinter.Entry(window, text=input_user)
input_field.pack(side=tkinter.BOTTOM, fill=tkinter.X)


def find_id_by_name(name, d):
    for k in d:
        if d[k] == name:
            return k
    return None


def print_str(string):
    messages.insert(tkinter.INSERT, string + '\n')


def enter_pressed(_):
    user_input = input_field.get()
    if user_input.startswith(':connect'):
        addr = user_input.split(' ')[1]
        port = int(user_input.split(' ')[2])
        connect(addr, port)
    elif user_input.startswith(':username'):
        global my_name
        my_name = user_input.split(' ')[1]
    elif user_input.startswith(':userslist'):
        users_list_request()
    elif user_input.startswith(':receiver'):
        global receiver_id
        receiver_id = find_id_by_name(user_input.split(' ')[1], users_list)
    else:
        print_str(my_name + '> ' + user_input)
        send_msg(user_input)

    input_user.set('')
    return "break"


def connect(addr, port):
    disconnect()
    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'ConnectRequest'
    msg.connect_request.u_id = my_id
    msg.connect_request.username = my_name

    global sock, s_addr, s_port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    window.createfilehandler(sock, tkinter.READABLE, got_message)
    sock.sendto(msg.SerializeToString(), (addr, port))
    s_addr = addr
    s_port = port


def send_msg(text):
    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'Message'
    msg.message.sender_id = my_id
    msg.message.receiver_id = receiver_id
    msg.message.text = text
    sock.sendto(msg.SerializeToString(), (s_addr, s_port))


def disconnect():
    global sock, s_addr, s_port
    if sock is not None:
        window.deletefilehandler(sock)
        sock.close()
        sock = s_addr = s_port = None


def users_list_request():
    msg = rolypoly_pb2.GenericMessage()
    msg.type = 'GetUserList'
    sock.sendto(msg.SerializeToString(), (s_addr, s_port))


def set_user_list(ul_proto):
    global users_list
    users_list = {}
    for i in range(len(ul_proto)):
        users_list[ul_proto[i].s_id] = ul_proto[i].username

    print_str('# List of users: #')
    for uid in users_list:
        print_str(str(users_list[uid]))


def got_message(s, _):
    data, _ = s.recvfrom(UDP_MAXLEN)
    msg = rolypoly_pb2.GenericMessage()
    msg.ParseFromString(data)
    if msg.type == 'Connected':
        print_str('# Connected succesfully #')
    elif msg.type == 'Ping':
        msg = rolypoly_pb2.GenericMessage()
        msg.type = 'Pong'
        msg.u_id = my_id
        sock.sendto(msg.SerializeToString(), (s_addr, s_port))
    elif msg.type == 'Message':
        n = users_list[msg.message.sender_id] if msg.message.sender_id in users_list else str(msg.message.sender_id)
        print_str(n + '> ' + msg.message.text)
    elif msg.type == 'UserList':
        set_user_list(msg.userlist.users)


frame = tkinter.Frame(window)  # , width=300, height=300)
input_field.bind("<Return>", enter_pressed)
frame.pack()

messages.insert(tkinter.INSERT, '# PyPoly Client #\n# ID: ' + str(my_id) + '\n')
window.mainloop()
disconnect()
