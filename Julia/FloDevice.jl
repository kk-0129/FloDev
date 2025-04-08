# 𝗙𝗟𝗢 : 𝗗𝗶𝘀𝘁𝗿𝗶𝗯𝘂𝘁𝗲𝗱 𝗛𝗶𝗲𝗿𝗮𝗿𝗰𝗵𝗶𝗰𝗮𝗹 𝗗𝗮𝘁𝗮𝗳𝗹𝗼𝘄 © 𝖪𝖾𝗏𝖾𝗇 𝖪𝖾𝖺𝗋𝗇𝖾𝗒 𝟮𝟬𝟮𝟯
##################################################
using Sockets
##################################################

#import Pkg;
#Pkg.add("StringEncodings")
#Pkg.add("DataStructures")

##################################################
# BYTES
##################################################

function readBytes(n::Integer,data::Vector{UInt8})
    if n == 0
        return (Vector{UInt8}(),data)
    elseif n >= length(data)
        return (data,Vector{UInt8}())
    else
        return (data[1:n],data[n+1:end])
    end
end

##################################################
# PRIMITIVES
##################################################

function encodeBool(b::Bool)
    return b ? Vector{UInt8}([ UInt8(1) ]) : Vector{UInt8}([ UInt8(0) ])
end

function decodeBool(bytes::Vector{UInt8})
    (bytes,rest) = readBytes(1,bytes)
    return ( bytes[1] > 0, rest )
end

function encodeData(data::Vector{UInt8})
    n = convert(UInt32, length(data))
    return Vector{UInt8}([ encodeUInt32(n); data ])
end

function decodeData(bytes::Vector{UInt8})
    (n,data_1) = decodeUInt32(bytes)
    return readBytes(n,data_1)
end

function encodeUInt8(u::UInt8)
    return Vector{UInt8}([ u ])
end

function decodeUInt8(bytes::Vector{UInt8})
    (bytes,rest) = readBytes(1,bytes)
    return ( bytes[1], rest )
end

function encodeUInt16(u::UInt16)
    return Vector{UInt8}([ UInt8(u & 0xFF ), UInt8( (u >> 8) & 0xFF ) ])
end

function decodeUInt16(data::Vector{UInt8})
    (bytes,rest) = readBytes(2,data)
    x = (UInt16(bytes[2]) << 8) | UInt16(bytes[1])
    return ( x, rest )
end

function encodeUInt32(u::UInt32)
    return Vector{UInt8}([
        UInt8( u & 0xFF ), # & 0xFF ascts as a cast to UInt8
        UInt8( (u >> 8) & 0xFF ),
        UInt8( (u >> 16) & 0xFF ),
        UInt8( (u >> 24) & 0xFF ),
    ])
end

function decodeUInt32(data::Vector{UInt8})
    (bytes,rest) = readBytes(4,data)
    x = (UInt32(bytes[4]) << 24) |
        (UInt32(bytes[3]) << 16) |
        (UInt32(bytes[2]) << 8) |
         UInt32(bytes[1])
    return ( x, rest )
end

function encodeUInt64(u::UInt64)
    return Vector{UInt8}([
        UInt8( u & 0xFF ), # & 0xFF ascts as a cast to UInt8
        UInt8( (u >> 8) & 0xFF ),
        UInt8( (u >> 16) & 0xFF ),
        UInt8( (u >> 24) & 0xFF ),
        UInt8( (u >> 32) & 0xFF ),
        UInt8( (u >> 40) & 0xFF ),
        UInt8( (u >> 48) & 0xFF ),
        UInt8( (u >> 56) & 0xFF ),
    ])
end

function decodeUInt64(data::Vector{UInt8})
    (bytes,rest) = readBytes(8,data)
    x = (UInt64(bytes[8]) << 56) |
        (UInt64(bytes[7]) << 48) |
        (UInt64(bytes[6]) << 40) |
        (UInt64(bytes[5]) << 32) |
        (UInt64(bytes[4]) << 24) |
        (UInt64(bytes[3]) << 16) |
        (UInt64(bytes[2]) << 8) |
         UInt64(bytes[1])
    return ( x, rest )
end

function encodeFloat32(f::Float32)
    return reinterpret(UInt8,[f])
end

function decodeFloat32(data::Vector{UInt8})
    (bytes,data_2) = readBytes(4,data)
    x = reinterpret(Float32,bytes)
    return ( x[1], data_2 )
end

using StringEncodings

function encodeString(s::String)
    data = StringEncodings.encode(s,"UTF-8")
    n = convert(UInt32, length(data))
    res = Vector{UInt8}([ encodeUInt32(n); data ])
    return res
end

function decodeString(data::Vector{UInt8})
    (len,data_2) = decodeUInt32(data)
    (bytes,data_3) = readBytes(len,data_2)
    s = StringEncodings.decode(bytes,"UTF-8")
    return ( s, data_3 )
end

##################################################
# TYPE
# id 1 = BOOL(BOOL?)     // default value
#    2 = DATA
#    3 = FLOAT(FLOAT?)   // default value
#    4 = STRING(STRING?) // default value
#    5 = ARRAY(TYPE)     // element type
#    6 = STRUCT(STRING,[STRING:TYPE]) // name, ivars
##################################################

struct TYPE   # see list above
    id::UInt8
    params
end

UNUSED = TYPE(0x00,nothing) # internal only
BOOL = TYPE(0x01,nothing)
DATA = TYPE(0x02,nothing)
FLOAT = TYPE(0x03,nothing)
STRING = TYPE(0x04,nothing)
function ARRAY(type::TYPE)
    return TYPE(0x05,type)
end

function encodeTYPE(type::TYPE)
    id = type.id
    params = type.params
    id_data = encodeUInt8(id)
    if id == 0x01 # BOOL
        if params == nothing
            return [ id_data; encodeBool(false) ]
        else
            return [ id_data; encodeBool(true); encodeBool(params) ]
        end
    elseif id == 0x02 # DATA
        return id_data
    elseif id == 0x03 # FLOAT
        if params == nothing
            return [ id_data; encodeBool(false) ]
        else
            return [ id_data; encodeBool(true); encodeFloat32(params) ]
        end
    elseif id == 0x04 # STRING
        if params == nothing
            return [ id_data; encodeBool(false) ]
        else
            return [ id_data; encodeBool(true); encodeString(params) ]
        end
    elseif id == 0x05 # ARRAY
        return [ id_data; encodeTYPE(params) ]
    elseif id == 0x06 # STRUCT
        (name,ivars) = params
        n = UInt8( length(ivars) )
        data = [ id_data; encodeString(name); encodeUInt8(n) ]
        for key in ivars.keys
            type = get(ivars,key,TYPE)
            data = [ data; encodeString(key); encodeTYPE(type) ]
        end
        return data
    end
    error("encodeTYPE: no type with id = $id")
end

function decodeTYPE(data::Vector{UInt8})
    (id,data_1) = decodeUInt8(data)
    if id == 0x01 # BOOL
        (has_default,data_2) = decodeBool(data_1)
        if has_default
            (default_value,data_3) = decodeBool(data_2)
            return ( TYPE(id,default_value), data_3 )
        else
            return ( TYPE(id,nothing), data_2 )
        end
    elseif id == 0x02 # DATA
        return ( TYPE(id,nothing), data_1 )
    elseif id == 0x03 # FLOAT
        (has_default,data_2) = decodeBool(data_1)
        if has_default
            (default_value,data_3) = decodeFloat32(data_2)
            return ( TYPE(id,default_value), data_3 )
        else
            return ( TYPE(id,nothing), data_2 )
        end
    elseif op == 0x04
        (has_default,data_2) = decodeBool(data_1)
        if has_default
            (default_value,data_3) = decodeString(data_2)
            return ( TYPE(id,default_value), data_3 )
        else
            return ( TYPE(id,nothing), data_2 )
        end
    elseif id == 0x05 # ARRAY
        (type,data_2) = decodeTYPE(data_1)
        return ( TYPE(id,type), data_2 )
    elseif id == 0x06 # STRUCT
        (name,data_2) = decodeString(data_1)
        (n,data_3) = decodeUInt8(data_2)
        ivars = Dict{String,TYPE}()
        rest = data_3
        for i in 1:n
            (key,data_4) = decodeString(rest)
            (value,data_5) = decodeTYPE(data_4)
            rest = data_5
            ivars[key] = value
        end
        return (TYPE(id,ivars), rest)
    end
    error("decodeTYPE: no type with id = $id")
end
        
##################################################
# PORTS
##################################################

using DataStructures

function encodePorts(ports::OrderedDict{String,TYPE})
    n = UInt8( length(ports) )
    data = encodeUInt8(n)
    for (key,type) in ports
        data = [ data; encodeString(key); encodeTYPE(type) ]
    end
    return data
end

function decodePorts(data::Vector{UInt8})
    (n,data_1) = decodeUInt8(data)
    ports = OrderedDict{String,TYPE}()
    rest = data_1
    for i in 1:n
        (key,data_2) = decodeString(rest)
        (value,data_3) = decodeTYPE(data_2)
        rest = data_3
        ports[key] = value
    end
    return (ports, rest)
end

##################################################
# STRUCT
##################################################

NIL_TYPE = TYPE(0x06,("nil",Dict{String,TYPE}()))
DATE_TYPE = TYPE(0x06,("Date",Dict{String,TYPE}([
        "year" => FLOAT,
        "month" => FLOAT,
        "day" => FLOAT,
        "hour" => FLOAT,
        "min" => FLOAT,
        "sec" => FLOAT
    ])))
XY_TYPE = TYPE(0x06,("XY",Dict{String,TYPE}([
        "x" => FLOAT,
        "y" => FLOAT
    ])))
XYZ_TYPE = TYPE(0x06,("XYZ",Dict{String,TYPE}([
        "x" => FLOAT,
        "y" => FLOAT,
        "z" => FLOAT
    ])))
EULER_TYPE = TYPE(0x06,("Euler",Dict{String,TYPE}([
        "pitch" => FLOAT,
        "yaw" => FLOAT,
        "roll" => FLOAT
    ])))
QUAT_TYPE = TYPE(0x06,("Quat",Dict{String,TYPE}([
        "angle" => FLOAT,
        "axis" => XYZ_TYPE
    ])))
__struct_types__ = Dict{String,TYPE}([
    "nil" => NIL_TYPE,
    "Date" => DATE_TYPE,
    "XY" => XY_TYPE,
    "XYZ" => XYZ_TYPE,
    "Euler" => EULER_TYPE,
    "Quat" => QUAT_TYPE
])

struct Struct
    type_name::String # name of struct type
    values::Dict{String,Any}
end

function struct_type(name::String)
    if haskey(__struct_types__,name)
        return __struct_types__[name]
    end
    error("no struct type called $name")
end

function encodeStruct(s::Struct)
    t = struct_type(s.type_name)
    if t != nothing
        return [ encodeString(s.type_name); encodeStruct(s,t) ]
    end
end

function encodeStruct(s::Struct,struct_type::TYPE)
    (name,ivars) = struct_type.params
    n = convert(UInt8, length(s.values))
    data = encodeUInt8(n)
    for (k,v) in s.values
        type = ivars[k]
        data = [ data ; encodeString(k); encodeValue(v,type) ]
    end
    return data
end

function decodeStruct(data::Vector{UInt8})
    (type_name,data_1) = decodeString(data)
    struct_type = struct_type(type_name)
    if struct_type != nothing
        return decodeStruct(data_1,struct_type)
    end
    return nothing
end

function decodeStruct(data::Vector{UInt8},struct_type::TYPE)
    (type_name,ivars) = struct_type.params
    (n,data_1) = decodeUInt8(data)
    d = data_1
    values = Dict{String,Any}
    for i in 1:n
        (key,data_2) = decodeString(d)
        type = ivars[key]
        (value,data_3) = decodeValue(data_2,type)
        d = data_3
        values[key] = value
    end
    return Struct(type_name,values)
end

##################################################
# SKIN
##################################################

struct Skin
    name::String
    inputs::OrderedDict{String,TYPE}
    outputs::OrderedDict{String,TYPE}
    meta::Struct
end

function encodeSkin(skin::Skin)
    name = encodeString(skin.name)
    inputs = encodePorts(skin.inputs)
    outputs = encodePorts(skin.outputs)
    meta = encodeStruct(skin.meta)
    return Vector{UInt8}([ name; inputs; outputs; meta ])
end

function decodeSkin(data::Vector{UInt8})
    (name,data_1) = decodeString(data)
    (inputs,data_2) = decodePorts(data_1)
    (outputs,data_3) = decodePorts(data_2)
    (meta,data_4) = decodeStruct(data_3)
    return ( Skin(name,inputs,outputs,meta), data_4 )
end

function encodeSkinArray(skins::Vector{Skin})
    n = convert(UInt32, length(skins))
    data = encodeUInt32(n)
    for s in skins
        data = [ data ; encodeSkin(s) ]
    end
    return data
end

function decodeSkinArray(data::Vector{UInt8})
    (n,data_2) = decodeUInt32(data)
    skins = Vector{Skin}()
    d = data_2
    for i in 1:n
        (skin,rest) = decodeSkin(d)
        d = rest
        push!(skins,skin)
    end
    return (skins,d)
end

##################################################
# EVENT
##################################################

struct Event
    value
    type
    meta
end

function encodeValue(value::Any,type::TYPE)
    id = type.id
    if id == 1 # BOOL
        return encodeBool(value)
    elseif id == 2 # DATA
        return encodeData(value)
    elseif id == 3 # FLOAT
        return encodeFloat32(value)
    elseif id == 4 # STRING
        return encodeString(value)
    elseif id == 5 # ARRAY
        element_type = type.params # TYPE
        n = convert(UInt32, length(value))
        data = encodeUInt32(n)
        for v in value
            data = [ data ; encodeValue(v,element_type) ]
        end
        return data
    elseif id == 6 # STRUCT
        return encodeStruct(value,type)
    end
end

function encodeEvent(event::Event)
    value = event.value
    meta = event.meta
    if value == nothing
        return encodeBool(false)
        if meta == nothing
            return [ encodeBool(false); encodeBool(false) ]
        else
            return [ encodeBool(false); encodeBool(true); encodeStruct(meta) ]
        end
    elseif event.meta == nothing
        return [ encodeBool(true); encodeValue(value, event.type); encodeBool(false) ]
    else
        return [ encodeBool(true); encodeValue(value, event.type); encodeBool(true); encodeStruct(meta) ]
    end
end

function decodeValue(data::Vector{UInt8},type::TYPE)
    id = type.id
    if id == 1 # BOOL
        return decodeBool(data)
    elseif id == 2 # DATA
        return decodeData(data)
    elseif id == 3 # FLOAT
        return decodeFloat32(data)
    elseif id == 4 # STRING
        return decodeString(data)
    elseif id == 5 # ARRAY
        element_type = type.params
        (n,data_1) = decodeUInt32(data)
        res = Vector{Any}()
        d = data_1
        for i in 1:n
            (value,data_2) = decodeValue(d,element_type)
            d = data_2
            push!(res,value)
        end
        return (res,d)
    elseif id == 6 # STRUCT
        return decodeStruct(data,type)
    end
end

function decodeEvent(data::Vector{UInt8},type::TYPE)
    (has_value,data_1) = decodeBool(data)
    if has_value
        (value,data_2) = decodeValue(data_1,type)
        (has_meta,data_3) = decodeBool(data_2)
        if has_meta
            (meta,data_4) = decodeStruct(data_3)
            return (Event(value,type,meta),data_4)
        else
            return (Event(value,type,nothing),data_3)
        end
    else
        (has_meta,data_2) = decodeBool(data_1)
        if has_meta
            (meta,data_3) = decodeStruct(data_2)
            return (Event(nothing,type,meta),data_3)
        else
            return (Event(nothing,type,nothing),data_2)
        end
    end
end

##################################################
# PAYLOADS
# op 0 = HANDSHAKE
#    1 = PING
#    2 = PING_UPDATE(name:String,skins:[Skin])
#    3 = SUBSCRIBE(skin:String,out:String,event:Event)
#    4 = END_SUBSCRIBE
#    5 = PUBLISH(skin:String,events:[String:Event])
##################################################

struct Payload # see list above
    op::UInt8
    content # for PING_UPDATE, SUBSCRIBE, PUBLISH
end

function encodePayload(payload::Payload)
    op = payload.op
    op_data = encodeUInt8(op)
    if op == 0 # HANDSHAKE
        return op_data
    elseif op == 1 # PING
        return op_data
    elseif op == 2 # PING_UPDATE(name:String,skins:[Skin])
        (name,skins) = payload.content
        return [ op_data; encodeString(name); encodeSkinArray(skins) ]
    elseif op == 3 # SUBSCRIBE(box:String,out:String,event:Event)
        (box,out,event) = payload.content
        return [ op_data; encodeString(box); encodeString(out); encodeEvent(event) ]
    elseif op == 4 # END_SUBSCRIBE
        return op_data
    elseif op == 5 # PUBLISH(box:String,events:[String:Event])
        # not used
    end
    error("encodePayload: no payload with op = $op")
end

function decodePayload(data::Vector{UInt8},skins::Dict{String,Skin})
    (op,data_1) = decodeUInt8(data)
    if op == 0x00 # HANDSHAKE
        return ( Payload(op,nothing), data_1 )
    elseif op == 0x01 # PING
        return ( Payload(op,nothing), data_1 )
    elseif op == 0x02 # PING_UPDATE(name:String,skins:[Skin])
        (name,data_2) = decodeString(data_1)
        (skins,data_3) = decodeSkinArray(data_2)
        return ( Payload(op,(name,skins)), data_3 )
    elseif op == 0x03 # SUBSCRIBE(box:String,out:String,event:Event)
        (box_name,data_2) = decodeString(data_1)
        (out_name,data_3) = decodeString(data_2)
        (event,data_4) = decodeEvent(data_3,UNUSED) # event.value is always nil !
        return ( Payload(op,(box_name,out_name,event)), data_4 )
    elseif op == 0x04 # END_SUBSCRIBE
        return ( Payload(op,nothing), data_1 )
    elseif op == 0x05 # PUBLISH(box:String,events:[String:Event])
        (box_name,data_2) = decodeString(data_1)
        skin = skins[box_name]
        (n,data_3) = decodeUInt8(data_2) # the number of encoded events
        events = Dict{String,Event}()
        d = data_3
        for i in 1:n
            (input_name,data_4) = decodeString(d)
            type = skin.inputs[input_name]
            (event,data_5) = decodeEvent(data_4,type)
            d = data_5
            events[input_name] = event
        end
        return ( Payload(op,(box_name,events)), d )
    end
    error("decodePayload: no payload with op = $op")
end

# this is required to index into ordered dicts
Base.getindex(h::Base.KeySet{K,<:OrderedDict{K}}, index) where K = h.dict.keys[index]

##################################################
# MESSAGES
##################################################

struct Message
    token::UInt64
    isReply::Bool
    payload::Payload
end

function encodeMessage(message::Message)
    return Vector{UInt8}([
         encodeUInt64(message.token);
         encodeBool(message.isReply);
         encodePayload(message.payload)
    ])
end

function decodeMessage(data::Vector{UInt8},skins::Dict{String,Skin})
    (token,data_1) = decodeUInt64(data)
    (isreply,data_2) = decodeBool(data_1)
    (payload,data_3) = decodePayload(data_2,skins)
    m = Message(token,isreply,payload)
    return (m,data_3)
end

##################################################
# SOCKET : SEND & RECEIVE PACKETS
##################################################

PKT_HEADER_LEN = 8 + 4 + 2
MAX_BUF_LEN = 8192
MAX_PKT_LEN = MAX_BUF_LEN - PKT_HEADER_LEN
PKT_N_FLAG = UInt16(0x8000)

__received_packets__ = Dict{UInt32,Dict{UInt16,Vector{UInt8}}}()

function receivedPacket(data::Vector{UInt8},skins::Dict{String,Skin})
    if length(data) > PKT_HEADER_LEN
        pkts = Dict{UInt16,Vector{UInt8}}()
        (c,rest) = decodeUInt32(data) # composition-id
        if haskey(__received_packets__,c)
            pkts = __received_packets__[c]
        end
        (idx,rest2) = decodeUInt16(rest)
        pkts[idx] = rest2 # store the packet data
        pkts_keys = keys(pkts)
        num_of_packets = length(pkts_keys)
        for n in pkts_keys
            if (n & PKT_N_FLAG) > 0
                n = n & ~PKT_N_FLAG # n = total number of packets
                if n == num_of_packets
                    # we have received all the packets ...
                    delete!(__received_packets__,c)
                    all_data = Vector{UInt8}()
                    pkts_keys = sort(collect(pkts_keys),rev=true) # highest = first, lowest = last
                    for i in pkts_keys
                        all_data = [ all_data; pkts[i] ]
                    end
                    return decodeMessage(all_data,skins)[1]
                end
            end
        end
        __received_packets__[c] = pkts # store the packet list
    end
    return nothing # this is ok, just haven't received enough packets yet
end

__socket__ = UDPSocket()
__pkt_comp_id :: UInt32 = 0
    
function sendMessage(msg::Message,host::IPv4,port::UInt16)
    global __pkt_comp_id += 1
    __pkt_comp_id_bytes = encodeUInt32(__pkt_comp_id) # id of packets
    encoded_msg = encodeMessage(msg)
    len_encoded_msg = length(encoded_msg)
    n = ceil( len_encoded_msg / MAX_PKT_LEN ) # number of packets to send ..
    for i = 0:(n-1)
        first = Integer(i * MAX_PKT_LEN) + 1
        last = Integer(min( len_encoded_msg, (i+1) * MAX_PKT_LEN ))
        idx = UInt16(n-i) # its a countdown
            if i == 0
                idx |= PKT_N_FLAG # first is flagged == gives the total number of packets !!
            end
        pkt = [ __pkt_comp_id_bytes; encodeUInt16(idx); encoded_msg[first:last] ]
        send(__socket__,host,port,pkt)
    end
end

##################################################
# SUBSCRIPTION
##################################################

struct Subscription
    host::IPv4
    port::UInt16
    skin_name::String
    out_name::String
end

__subscriptions__ = Dict{UInt64,Subscription}()

##################################################
# BLACK BOX
##################################################

struct BlackBox
    skin::Skin
    publish # a function (Dict{String,Event})->()
    output # a function (String)->(Event)
end

function deviceCallback(skin_name::String,events::Dict{String,Event})
    for (out_name,event) in events
        for (token,sub) in __subscriptions__
            if skin_name == sub.skin_name && out_name == sub.out_name
                msg = Message(token,true,Payload(0x03,(skin_name,out_name,event)))
                sendMessage(msg,sub.host,sub.port)
            end
        end
    end
end

##################################################
# DEVICE
##################################################

__clients__ = Dict{String,Bool}()

function launchDevice(name::String,blackboxes::Vector{BlackBox},port::UInt16)
    
    # bind the socket ...
	host = getipaddr() # outtdated: should use getipaddrs(IPv4) instead
    bind(__socket__,host,port)
    println("IP addr = ",host,":",port)
	println("listening for incoming messages ...")

    skin_list::Vector{Skin} = map( x -> x.skin, blackboxes )
    
    println(skin_list)
    
    boxes = Dict{String,BlackBox}()
    skins = Dict{String,Skin}()
    for b in blackboxes
        boxes[b.skin.name] = b
        skins[b.skin.name] = b.skin
    end
	
    # start listening for messages ...
	while true
		(addr, data_1) = recvfrom(__socket__) # data_1 is Vector{UInt8}
		if length(data_1) >= 4
   
            # sender ...
			host = addr.host
			port = addr.port
			client_key = "$host:$port"
            
            # token ...
            request = receivedPacket(data_1,skins)
   
            # message
            reply_payload = nothing
            if request == nothing
                # ignore : not received enough packets
            else
                op = request.payload.op
                if op == UInt8(0) # HANDSHAKE
                    __clients__[client_key] = true
                    reply_payload = Payload(0x00,nothing)
                    #println("reply HANDSHAKE to ",client_key)
                elseif haskey(__clients__,client_key)
                    needs_update = __clients__[client_key]
                    
                    if op == UInt8(1) # PING
                        if needs_update
                            __clients__[client_key] = false
                            #println("reply PING_UPDATE to ", client_key)
                            reply_payload = Payload(0x02,(name,skin_list))
                        else
                            reply_payload = Payload(0x01,nothing)
                        end
                    
                    elseif op == UInt8(2) # PING_UPDATE
                        # ignore : not a valid request
                    
                    elseif op == UInt8(3) # SUBSCRIBE(box:String,out:String,event:Event)
                        println("received SUBSCRIBE request from ", client_key)
                        (box_name,out_name,event) = request.payload.content
                        black_box = boxes[box_name]
                        sub = Subscription(addr.host,addr.port,box_name,out_name)
                        __subscriptions__[request.token] = sub
                        event = black_box.output( out_name )
                        if event != nothing
                            reply_payload = Payload(0x03,(box_name,out_name,event))
                        end
                        
                    elseif op == UInt8(4) # END_SUBSCRIBE
                        println("received END_SUBSCRIBE request from ", client_key)
                        delete!(__subscriptions__, request.token)
                    
                    elseif op == UInt8(5) # PUBLISH(box:String,events:[String:Event])
                        (box_name,events) = request.payload.content
                        black_box = boxes[box_name]
                        black_box.publish(events)
                        # no reply required
                    else
                        println("received INVALID request")
                    end
                end
            end
            
            # send the reply ...
            if reply_payload == nothing
                # ignore it
            else
                reply = Message(request.token,true,reply_payload)
                sendMessage(reply,addr.host,addr.port)
            end
            
		end # of IF
	end # of WHILE
    
end # of launch


