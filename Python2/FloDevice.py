# 𝗙𝗟𝗢 : 𝗗𝗶𝘀𝘁𝗿𝗶𝗯𝘂𝘁𝗲𝗱 𝗛𝗶𝗲𝗿𝗮𝗿𝗰𝗵𝗶𝗰𝗮𝗹 𝗗𝗮𝘁𝗮𝗳𝗹𝗼𝘄 © 𝖪𝖾𝗏𝖾𝗇 𝖪𝖾𝖺𝗋𝗇𝖾𝗒 𝟮𝟬𝟮𝟯
##################################################
import struct
from collections import OrderedDict
import math
import thread
from threading import Lock
import time
import socket
import ctypes
##################################################

##################################################
# BYTES
##################################################
def readBytes(n,data):
    if n == 0:
        return ([], data)
    if n >= len(data):
        return (data, [])
    return (data[0:n], data[n:])

##################################################
# PRIMITIVES
##################################################

# Bool --------------------------------------------------------------------
def encodeBool(b):
    if b == True:
        return [0x01]
    return [0x00]

def decodeBool(data):
    (data,rest) = readBytes(1,data)
    return ( data[0] > 0, rest )

# Data --------------------------------------------------------------------
def encodeData(data):
    #print("data = ",data)
    n = len(data)# & 0xFFFFFFFF # convert to UInt32
    #print("type n = ",type(encodeUInt32(n)))
    #print("type data = ",type(data))
    return encodeUInt32(n) + data

def decodeData(data):
    (n,rest_1) = decodeUInt32(data)
    return readBytes(n,rest_1)

# UInt8 --------------------------------------------------------------------
def encodeUInt8(u):
    return [ u ]

def decodeUInt8(data):
    (data,rest) = readBytes(1,data)
    return ( data[0], rest )

# UInt16 --------------------------------------------------------------------
def encodeUInt16(u):
    return [ u & 0xFF, (u >> 8) & 0xFF ]

def decodeUInt16(data):
    (data,rest) = readBytes(2,data)
    x = (data[1] << 8) | data[0]
    return ( x, rest )
    
# UInt32 --------------------------------------------------------------------
def encodeUInt32(u):
    return [ u & 0xFF, (u >> 8) & 0xFF, (u >> 16) & 0xFF, (u >> 24) & 0xFF ]

def decodeUInt32(data):
    (data,rest) = readBytes(4,data)
    x = (data[3] << 24) | (data[2] << 16) | (data[1] << 8) | data[0]
    return ( x, rest )
    
# UInt64 --------------------------------------------------------------------
def encodeUInt64(u):
    return [ u & 0xFF, (u >> 8) & 0xFF, (u >> 16) & 0xFF, (u >> 24) & 0xFF, (u >> 32) & 0xFF, (u >> 40) & 0xFF, (u >> 48) & 0xFF, (u >> 56) & 0xFF ]

def decodeUInt64(data):
    (data,rest) = readBytes(8,data)
    x = (data[7] << 56) | (data[6] << 48) | (data[5] << 40) | (data[4] << 32) | (data[3] << 24) | (data[2] << 16) | (data[1] << 8) | data[0]
    return ( x, rest )

# Float32 --------------------------------------------------------------------
def encodeFloat32(f):
    return list( struct.pack("f", f) )

def decodeFloat32(data):
    (data,rest) = readBytes(4,data)
    return ( struct.unpack('>f', bytearray( reversed(data[0:4]) ))[0], rest )
    
# String --------------------------------------------------------------------
def encodeString(s):
    e = list(s.encode("utf-8"))
    return encodeData(e)
    
def decodeString(data):
    (d,rest) = decodeData(data)
    x = bytearray(d).decode('utf-8')
    return (x, rest)

##################################################
# TYPE
# id 1 = BOOL(BOOL?)     // default value
#    2 = DATA
#    3 = FLOAT(FLOAT?)   // default value
#    4 = STRING(STRING?) // default value
#    5 = ARRAY(TYPE)     // element type
#    6 = STRUCT(STRING,[STRING:TYPE]) // name, ivars
##################################################

# TYPE = tuple ( id, params )

BOOL = 0x01
DATA = 0x02
FLOAT = 0x03
STRING = 0x04
ARRAY = 0x05
STRUCT = 0x06

def encodeStringTypeDict(d):
    n = len(d) & 0xFF # convert to UInt8
    data = encodeUInt8(n)
    for (key,type) in d.items():
        data += encodeString(key) + encodeTYPE(type)
    return data
    
def encodeTYPE(type):
    (id,params) = type
    id_data = encodeUInt8(id)
    if (id == BOOL): # BOOL
        if params == None:
            return id_data + encodeBool(False)
        else:
            return id_data + encodeBool(True) + encodeBool(params)
    elif (id == DATA): # DATA
        return id_data
    elif (id == FLOAT): # FLOAT
        if params == None:
            return id_data + encodeBool(False)
        else:
            return id_data + encodeBool(True) + encodeFloat32(params)
        end
    elif (id == STRING): # STRING
        if params == None:
            return id_data  + encodeBool(False)
        else:
            return id_data  +  encodeBool(True)  + encodeString(params)
        end
    elif (id == ARRAY): # ARRAY
        return id_data + encodeTYPE(params)
    elif (id == STRUCT): # STRUCT
        (name,ivars) = params
        return id_data + encodeString(name) + encodeStringTypeDict(ivars)

def decodeStringTypeDict(data,d): # d = Dict() or OrderedDict()
    (n,rest_1) = decodeUInt8(data)
    rest = rest_1
    for i in range(n):
        (key,rest_2) = decodeString(rest)
        (type,rest_3) = decodeTYPE(rest_2)
        d[key] = type
        rest = rest_3
    return (d, rest)

def decodeTYPE(data):
    (id,rest_1) = decodeUInt8(data)
    if (id == BOOL): # BOOL
        (has_default,rest_2) = decodeBool(rest_1)
        if has_default:
            (default_value,rest_3) = decodeBool(rest_2)
            return ( (id,default_value), rest_3 )
        else:
            return ( (id,None), rest_2 )
    elif (id == DATA): # DATA
        return ( (id,None), rest_1)
    elif (id == FLOAT): # FLOAT
        (has_default,rest_2) = decodeBool(rest_1)
        if has_default:
            (default_value,rest_3) = decodeFloat32(rest_2)
            return ( (id,default_value), rest_3 )
        else:
            return ( (id,None), rest_2 )
    elif (id == STRING): # STRING
        (has_default,rest_2) = decodeBool(rest_1)
        if has_default:
            (default_value,rest_3) = decodeString(rest_2)
            return ( (id,default_value), rest_3 )
        else:
            return ( (id,None), rest_2 )
    elif (id == ARRAY): # ARRAY
        (element_type,rest_2) = decodeTYPE(rest_1)
        return ( (id,element_type), rest_2 )
    elif (id == STRUCT): # STRUCT
        (name,rest_2) = decodeString(rest_1)
        (ivars,rest_3) = decodeStringTypeDict(rest_2,{})
        return ( (id,(name,ivars)), rest_3)
        
##################################################
# PORTS
##################################################

def encodePorts(ports): # OrderedDict{String,TYPE}
    return encodeStringTypeDict(ports)

def decodePorts(data):
    return decodeStringTypeDict(data,OrderedDict())

##################################################
# SKIN
##################################################

# Skin = (String,Ports,Ports) // name, inputs, outputs

def encodeSkin(skin):
    (name,inputs,outputs,metadata) = skin
    return encodeString(name) + encodePorts(inputs) + encodePorts(outputs) + encodeStruct(metadata)
    
def decodeSkin(data):
    (name,rest_1) = decodeString(data)
    (inputs,rest_2) = decodePorts(rest_1)
    (outputs,rest_3) = decodePorts(rest_2)
    (metadata,rest_4) = decodePorts(rest_3)
    return ( (name,inputs,outputs), rest_4 )
    
def encodeSkinArray(skins):
    n = len(skins) & 0xFFFFFFFF # convert to UInt32
    data = encodeUInt32(n)
    for s in skins:
        data += encodeSkin(s)
    return data
    
def decodeSkinArray(data):
    (n,rest_1) = decodeUInt32(data)
    skins = []
    rest = rest_1
    for i in range(n):
        (skin,rest_1) = decodeSkin(rest)
        skins.append(skin)
        rest = rest_1
    return ( skins, rest )

##################################################
# VALUES - used by STRUCT & EVENT ..
##################################################

def encodeValue(value,type): # any value, (type_id,type_params)
    (id,params) = type
    if (id == BOOL): # BOOL
        return encodeBool(value)
    elif (id == DATA): # DATA
        return encodeData(value)
    elif (id == FLOAT): # FLOAT
        return encodeFloat32(value)
    elif (id == STRING): # STRING
        return encodeString(value)
    elif (id == ARRAY): # ARRAY
        # value = array, params = element_type
        n = len(value) & 0xFFFFFFFF # convert to UInt32
        data = encodeUInt32(n)
        for v in value:
            data += encodeValue(v,params)
        return data
    elif (id == STRUCT): # STRUCT
        return encodeStructWithType(value,type)
    return None # error
    
def decodeValue(data,type):
    (id,params) = type
    if (id == BOOL): # BOOL
        return decodeBool(data)
    elif (id == DATA): # DATA
        return decodeData(data)
    elif (id == FLOAT): # FLOAT
        return decodeFloat32(data)
    elif (id == STRING): # STRING
        return decodeString(data)
    elif (id == ARRAY): # ARRAY
        # value = array, params = element_type
        (n,data_1) = decodeUInt32(data)
        res = []
        d = data_1
        for i in range(n):
            (value,data_2) = decodeValue(d,params)
            d = data_2
            res.append(value)
        return (res,d)
    elif (id == STRUCT): # STRUCT
        res = decodeStructWithType(data,type)
        #print("decoded ",type," ----> ",res)
        return res
    return None # error

##################################################
# STRUCT
##################################################

# struct = (String,Dict{String,Any}) // (type_name,values)

NIL_TYPE = (STRUCT,("nil",{}))
DATE_TYPE = (STRUCT,("Date",{ # 0x06 = STRUCT
    "year" : (FLOAT,None), # 0x03 = FLOAT
    "month" : (FLOAT,None),
    "day" : (FLOAT,None),
    "hour" : (FLOAT,None),
    "min" : (FLOAT,None),
    "sec" : (FLOAT,None)
}))
XY_TYPE = (STRUCT,("XY",{
    "x" : (FLOAT,None),
    "y" : (FLOAT,None)
}))
XYZ_TYPE = (STRUCT,("XYZ",{
    "x" : (FLOAT,None),
    "y" : (FLOAT,None),
    "z" : (FLOAT,None)
}))
EULER_TYPE = (STRUCT,("Euler",{
    "pitch" : (FLOAT,None),
    "yaw" : (FLOAT,None),
    "roll" : (FLOAT,None)
}))
QUAT_TYPE = (STRUCT,("XYZ",{
    "angle" : (FLOAT,None),
    "axis" : XYZ_TYPE
}))
AUDIO_TYPE = (STRUCT,("Audio",{
    "index" : (FLOAT,None), #float
    "channels" : (FLOAT,None),
    "samplerate" : (FLOAT,None),
    "format" : (STRING,None), #string
    "interleaved" : (BOOL,None), #bool
    "data" : (DATA,None) #data
}))

__struct_types__ = {
    "nil" : NIL_TYPE,
    "Date" : DATE_TYPE,
    "XY" : XY_TYPE,
    "XYZ" : XYZ_TYPE,
    "Euler" : EULER_TYPE,
    "Quat" : QUAT_TYPE,
    "Audio" : AUDIO_TYPE
}

def structType(name):
    return __struct_types__[name]
    
def encodeStructWithType(s,struct_type):
    (type_name,values) = s
    (_,(_,ivars)) = struct_type # (0x06,(name,ivars))
    data = encodeString(type_name)
    n = len(values) & 0xFF # convert to UInt8
    data += encodeUInt8(n)
    for (k,_v) in values.items():
        (v,type) = _v
        #type = ivars[k]
        data += encodeString(k) + encodeValue(v,type)
    return data

def encodeStruct(s):
    print(s)
    (type_name,_) = s
    struct_type = __struct_types__[type_name]
    if struct_type != None:
        return encodeStructWithType(s,struct_type)
    return None

def decodeStructWithType(data,struct_type):
    (_,(type_name,ivars)) = struct_type
    (type_name_2,rest_1) = decodeString(data)
    (n,rest_2) = decodeUInt8(rest_1)
    rest = rest_2
    values = {}
    for i in range(n):
        (key,rest_3) = decodeString(rest)
        type = ivars[key]
        (value,rest_4) = decodeValue(rest_3,type)
        values[key] = value
        rest = rest_4
    return ((type_name,values),rest)

#def decodeStruct(data):
#    (type_name,data_1) = decodeString(data)
#    struct_type = struct_type(type_name)
#    if struct_type != None:
#        return decodeStructWithType(data_1,struct_type)
#    return None

##################################################
# EVENT
##################################################

# event = (Any,TYPE) // (value,type), value may be None, type is not serialised

def encodeEvent(event):
    (value,type,meta) = event
    if value == None:
        if meta == None:
            return encodeBool(False) + encodeBool(False)
        else:
            return encodeBool(False) + encodeBool(True) + encodeStruct(meta)
    if meta == None:
        return encodeBool(True) + encodeValue(value,type) + encodeBool(False)
    else:
        return encodeBool(True) + encodeValue(value,type) + encodeBool(True) + encodeStruct(meta)

def decodeEvent(data,type):
    (has_value,data_1) = decodeBool(data)
    if has_value:
        (value,data_2) = decodeValue(data_1,type)
        (has_meta,data_3) = decodeBool(data_2)
        if has_meta:
            (meta,data_4) = decodeStruct(data_3)
            return ((value,type,meta),data_4)
        else:
            return ((value,type,None),data_3)
    else:
        (has_meta,data_2) = decodeBool(data_1)
        if has_meta:
            (meta,data_3) = decodeStruct(data_2)
            return ((None,type,meta),data_3)
        else:
            return ((None,type,None),data_2)

##################################################
# PAYLOADS
# op 0 = HANDSHAKE
#    1 = PING
#    2 = PING_UPDATE(name:String,skins:[Skin])
#    3 = SUBSCRIBE(skin:String,out:String,event:Event)
#    4 = END_SUBSCRIBE
#    5 = PUBLISH(skin:String,inputs:[String:Event])
##################################################

# payload = (op,content) # content only for PING_UPDATE, SUBSCRIBE, PUBLISH (above)

def encodePayload(payload):
    (op,content) = payload
    op_data = encodeUInt8(op)
    if (op == 0x00): # HANDSHAKE
        return op_data
    elif (op == 0x01): # PING
        return op_data
    elif (op == 0x02): # PING_UPDATE
        (name,skins) = content
        return op_data + encodeString(name) + encodeSkinArray(skins)
    elif (op == 0x03): # SUBSCRIBE
        (skin,out,event) = content
        return op_data + encodeString(skin) + encodeString(out) + encodeEvent(event)
    elif (op == 0x04): # END_SUBSCRIBE
        return op_data
    elif (op == 0x05): # PUBLISH
        return None # not used for devices

def decodePayload(data,boxes): # boxes is the dictionary of device boxes
    (op,data_1) = decodeUInt8(data)
    if (op == 0x00): # HANDSHAKE
        return ( (op,None), data_1 )
    elif (op == 0x01): # PING
        return ( (op,None), data_1 )
    elif (op == 0x02): # PING_UPDATE
        (name,data_2) = decodeString(data_1)
        (skins,data_3) = decodeSkinArray(data_2)
        return ( (op,(name,skins)), data_3 )
    elif (op == 0x03): # SUBSCRIBE
        (skin_name,data_2) = decodeString(data_1)
        (output_name,data_3) = decodeString(data_2)
        (event,data_4) = decodeEvent(data_3,None) # event.value is always nil
        return ( (op,(skin_name,output_name,event)), data_4 )
    elif (op == 0x04): # END_SUBSCRIBE
        return ( (op,None), data_1 )
    elif (op == 0x05): # PUBLISH
        (skin_name,data_2) = decodeString(data_1)
        box = boxes[skin_name]
        if (box != None):
            (_,inputs,_,_) = box.skin
            (n,data_3) = decodeUInt8(data_2)
            events = {} # empty dictionary
            d = data_3
            for i in range(n):
                (input_name,data_4) = decodeString(d)
                input_type = inputs[input_name]
                (event,data_5) = decodeEvent(data_4,input_type)
                d = data_5
                events[input_name] = event
            return ( (op,(skin_name,events)), d )
        else:
            print("ERROR: decodePayload - no box named ",skin_name)
            return None

##################################################
# MESSAGES
##################################################

# message = (UInt32,Bool,Payload) // (token,is_reply,payload)

def encodeMessage(message):
    (token,is_reply,payload) = message
    return encodeUInt64(token) + encodeBool(is_reply) + encodePayload(payload)

def decodeMessage(data,boxes): # boxes is the list of device boxes
    (token,data_1) = decodeUInt64(data)
    (is_reply,data_2) = decodeBool(data_1)
    (payload,data_3) = decodePayload(data_2,boxes)
    return ( (token,is_reply,payload) ,data_3)

##################################################
# SOCKET : SEND & RECEIVE PACKETS
##################################################

MAX_BUF_LEN = 8192
PKT_HEADER_LEN_NO_TOKEN = 4 + 2
PKT_HEADER_LEN = 8 + PKT_HEADER_LEN_NO_TOKEN
MAX_PKT_LEN = MAX_BUF_LEN - PKT_HEADER_LEN
PKT_N_FLAG = 0x8000 

__received_packets__ = {} # {UInt32:{UInt16:[UInt8]}} = {pk_list_id:{packet_index:[packet_bytes]}}

def receivedPacket(data,boxes):
    if len(data) >= PKT_HEADER_LEN_NO_TOKEN:
        pkts = {} # {UInt16:[UInt8]}
        (c,rest) = decodeUInt32(data) # composition-id
        if c in __received_packets__:
            pkts = __received_packets__[c]
        (idx,rest2) = decodeUInt16(rest)
        pkts[idx] = rest2 # store the packet
        pkts_keys = pkts.keys()
        num_of_packets = len(pkts_keys)
        for n in pkts_keys:
            if (n & PKT_N_FLAG) > 0:
                n = n & ~PKT_N_FLAG # n = total number of packets
                if n == num_of_packets:
                    # we have all the packets ...
                    if c in __received_packets__:
                        del __received_packets__[c]
                    all_data = [] # [UInt8]
                    pkts_keys = reversed(sorted(pkts_keys)) # highest = first, lowest = last
                    for i in pkts_keys:
                        all_data += pkts[i]
                    (msg,_) = decodeMessage(all_data,boxes)
                    return msg
        __received_packets__[c] = pkts # store the packet list
    print("len(data) = ",len(data),", PKT_HEADER_LEN = ",PKT_HEADER_LEN_NO_TOKEN)
    return None # this is ok, just haven't received enough packets yet

##################################################
# BLACK BOX
##################################################

class BlackBox(object):

    def __init__(self,skin):
        self.skin = skin # Skin = (String,Ports,Ports,Struct) // name, inputs, outputs, meta
        self.device = None
        
    def setDevice(self,device):
        self.device = device
        
    def publish(self,events): # a function ( {String:Event} ) -> ()
        raise NotImplementedError("publish not implemented")
    
    def output(name): # a function ( String ) -> ( Event )
        raise NotImplementedError("output not implemented")

##################################################
# DEVICE
##################################################

class Device:

    def __init__(self,device_name,blackboxes,address): # String, [BlackBox], (String,UInt16)
        # bind the socket ...
        (host,port) = address
        if (host == "local"):
            host = socket.gethostname() # makes socket visible to other machines
        print("launching device '",device_name,"' @ ",host,":",port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)#, socket.IPPROTO_UDP)
        self.sock.bind((host,port))
        self.mutex = Lock()
    
        #boxes::Vector{Box} = map( x -> x.box, blackboxes )
        self.name = device_name
        self.skins = []
        self.boxes = {}
        for b in blackboxes:
            b.setDevice(self)
            (name,_,_,_) = b.skin
            self.boxes[name] = b
            self.skins.append(b.skin)
            
        self.clients = {} # {String:Bool} # known clients
        # subscription = (AF_INET,String,String) // ((host,port),skin_name,out_name)
        self.subscriptions = {} # {UInt32:Subscription}
        self.pk_counter = 0
        
    def sendMessage(self,msg,addr):
        self.pk_counter += 1
        pk_counter_bytes = encodeUInt32(self.pk_counter)
        #token_bytes = encodeUInt64(token) # token required for each packet
        encoded_msg = encodeMessage(msg)
        len_encoded_msg = len(encoded_msg)
        c = math.ceil(float(len_encoded_msg)/float(MAX_PKT_LEN))
        #let n = Int(ceil(Float32(bytes.count)/Float32(Packet.MAX_PKT_LEN))) // number of packets to send ..
        n = int(c) # number of packets to send ..
        for i in range(n):
            first = int(i * MAX_PKT_LEN)
            last = int(min( len_encoded_msg, (i+1) * MAX_PKT_LEN ))
            idx = n-i# & 0xFFFF # its a countdown
            if i == 0:
               idx |= PKT_N_FLAG # first is flagged == gives the total number of packets !!
            #pkt = token_bytes + encodeUInt16(idx) + encoded_msg[first:last]
            pkt = pk_counter_bytes + encodeUInt16(idx) + encoded_msg[first:last]
            #print("send-reply to: ",addr)
            self.sock.sendto(bytearray(pkt),addr)
        
    def sendSubscriptionReply(self,token,skin_name,out_name,event,addr):
        msg = (token,True,(0x03,(skin_name,out_name,event)))
        self.sendMessage(msg,addr)
                
    def callback(self,box,out_name,event):
        (skin_name,_,_,_) = box.skin
        for (token,sub) in self.subscriptions.iteritems():
            (addr,sub_skin_name,sub_out_name) = sub
            if (skin_name == sub_skin_name and out_name == sub_out_name):
                self.sendSubscriptionReply(token,skin_name,out_name,event,addr)

    def launch(self):
        
        # start listening for messages ...
        print("listening for incoming messages ..")
        while True:
            data, addr = self.sock.recvfrom(MAX_BUF_LEN)
            data_1 = bytearray(data)
            if len(data_1) >= 4:
                (sender_host,sender_port) = addr
                #print("rcvd msg from",sender_host,":",sender_port)
                request = receivedPacket(data_1,self.boxes)
                reply_payload = None
                if (request == None):
                    print("msg IS NOT A REQUEST")
                else:
                    client_key = "" + sender_host + ":" + str(sender_port)
                    (token,is_reply,payload) = request
                    if (not is_reply):
                        (op,payload_args) = payload
                        if (op == 0x00): # HANDSHAKE
                            print("rcvd HANDSHAKE #",token," from",sender_host,":",sender_port)
                            self.clients[client_key] = True
                            reply_payload = (0x00,None)
                            print("reply HANDSHAKE to", client_key)
                        elif (client_key in self.clients):
                            needs_update = self.clients[client_key]
                            if (op == 0x01):
                                #print("rcvd PING from",sender_host,":",sender_port)
                                if (needs_update):
                                    self.clients[client_key] = False
                                    reply_payload = (0x02,(self.name,self.skins))
                                    #print("reply PING_UPDATE to", client_key)
                                else:
                                    reply_payload = (0x01,None)
                            elif (op == 0x03):
                                print("received SUBSCRIBE request from ", client_key)
                                (skin_name,out_name,event) = payload_args
                                black_box = self.boxes[skin_name]
                                with self.mutex:
                                    self.subscriptions[token] = (addr,skin_name,out_name)
                                event = black_box.output( out_name )
                                if (event != None):
                                    reply_payload = (0x03,(skin_name,out_name,event))
                            elif (op == 0x04):
                                print("received END_SUBSCRIBE request from ", client_key)
                                with self.mutex:
                                    if token in self.subscriptions:
                                        del self.subscriptions[token]
                            elif (op == 0x05):
                                #print("rcvd PUBLISH from",sender_host,":",sender_port)
                                (skin_name,events) = payload_args
                                box = self.boxes[skin_name]
                                if (box != None):
                                    box.publish(events)
                                else:
                                    print("-> publish to unknown box: ", skin_name)
                            else:
                                print("Received invalid request op = ", str(op))
                        else:
                            print("Unknown client: ", client_key)
                if (reply_payload != None):
                    #print("- reply token = #",token)
                    reply = (token,True,reply_payload)
                    self.sendMessage(reply,addr)
