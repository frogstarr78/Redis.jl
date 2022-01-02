module Redis

import Base: TcpSocket, IPAddr
export client

CRLFc = ['\r', '\n']
CRLF = join(CRLFc)
EXPECTATIONS = Dict{DataType,Set}(
  Array => Set([ "hkeys", "hvals", "hmget", "keys", "lrange", "mget" ]),
  Bool => Set([
    "del",
    "expire",
    "expireat",
    "pexpire",
    "pexpireat",
    "hset",
    "move",
    "msetnx",
    "persist",
    "sismember"
  ]),
  Dict => Set(["hgetall"]),
  Float64 => Set([
    "incrbyfloat",
    "hincrbyfloat",
    "zadd",
    "zscore"
  ]),
  Int => Set([
    "append",
    "bitcount",
    "bitpos",
    "dbsize",
    "decr",
    "decrby",
    "exists",
    "getbit",
    "hexists",
    "hlen",
    "hincrby",
    "hstrlen",
    "incr",
    "incrby",
    "linsert",
    "llen",
    "lpush",
    "lpushx",
    "lrem",
    "pttl",
    "rpush",
    "rpushx",
    "scard",
    "sadd",
    "setbit",
    "setrange",
    "smove",
    "srem",
    "strlen",
    "sunionstore",
    "ttl",
    "zadd",
    "zcard",
    "zcount",
    "zrank"
  ]),
  Set => Set([
    "smembers",
    "srandmember",
    "sunion"
  ]),
  AbstractString => Set([
    "auth",
    "bgrewriteaof",
    "bgsave",
    "del",
    "echo",
    "flushall",
    "flushdb",
    "get",
    "getrange",
    "getset",
    "hget",
    "hmset",
    "info", 
    "lindex",
    "lpop",
    "lset",
    "ltrim",
    "mset",
    "ping",
    "quit",
    "randomkey",
    "rename",
    "renamenx",
    "rpop",
    "rpoplpush",
    "save",
    "select",
    "set",
    "spop",
    "type"
  ]),
  Libc.TmStruct => Set(["lastsave", "time"])
)

redis_type(arg::AbstractString; simple::Bool=false) = (simple ? string('+', arg, CRLF ) : string('$', join([length(arg), arg], CRLF), CRLF))
redis_type(arg::Union)                              = string('$', -1, CRLF)
redis_type(arg::Number)                             = string(':', arg, CRLF)
function redis_type(arg::Array)
  arg[1] = uppercase(arg[1])
  len = length(arg)
  if len == 1
    return string('*', len, CRLF, redis_type(arg[1]))
  elseif len > 1
    return string('*', len, CRLF, mapreduce(redis_type, *, arg))
  end
end
#redis_type(arg::Tuple)                      = collect(arg)

struct UnAuthException <: Exception end
struct ErrorException <: Exception
  detail::AbstractString
end

parse_error_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)   = throw(ErrorException(resp[2:end]))
function parse_string_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)
  if lowercase(cmd_called) in EXPECTATIONS[AbstractString] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
    return resp[2:end]
  else
#   @printf "+cmd_called '%s'\n" cmd_called
    throw(KeyError(EXPECTATIONS))
  end
end

function parse_int_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)
  if lowercase(cmd_called) in EXPECTATIONS[Int] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
    return parse(Int,resp[2:end])
  elseif lowercase(cmd_called) in EXPECTATIONS[Libc.TmStruct]
    return Libc.TmStruct(resp[2:end])
  elseif lowercase(cmd_called) in EXPECTATIONS[Bool] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
    return parse(Int,resp[2:end]) > 0
  else
#    @printf ":cmd_called '%s'\n" cmd_called
    throw(KeyError(EXPECTATIONS))
  end
end

function parse_bulk_string_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)
#  @printf "sub_parse '%s', in EXPECTATIONS '%s'\n" sub_parse ( lowercase(cmd_called) in EXPECTATIONS[Array] || lowercase(cmd_called) in EXPECTATIONS[Set] || lowercase(cmd_called) in EXPECTATIONS[Dict] )
  if lowercase(cmd_called) in EXPECTATIONS[AbstractString] || lowercase(cmd_called) in EXPECTATIONS[Set] || ( sub_parse && ( lowercase(cmd_called) in EXPECTATIONS[Array] || lowercase(cmd_called) in EXPECTATIONS[Dict] ) )
    len = parse(Int,resp[2:end])
    if len == -1
      return ""
    else
      r = join(map(Char,readbytes(sock, len)))
      readuntil(sock, CRLF)
      return r
    end
  elseif lowercase(cmd_called) in EXPECTATIONS[Float64] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
    len = parse(Int,resp[2:end])
    r = join(map(Char,readbytes(sock, len)))
    readuntil(sock, CRLF)
    return parse(Float64,r)
  elseif lowercase(cmd_called) in EXPECTATIONS[Libc.TmStruct] && sub_parse
    len = parse(Int,resp[2:end])
    r = join(map(Char,readbytes(sock, len)))
    readuntil(sock, CRLF)
    return r
  else
#    @printf "\$cmd_called '%s'\n" cmd_called
    throw(KeyError(EXPECTATIONS))
  end
end

function parse_array_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)
  len = parse(Int,resp[2:end])
  if lowercase(cmd_called) in EXPECTATIONS[Dict]
    r = [decode_response(sock, cmd_called, true) for i = 1:len]
    ret = Dict{AbstractString,Any}()
    while !isempty(r)
      merge!(ret, Dict(shift!(r) => shift!(r)))
    end
    return ret
  elseif lowercase(cmd_called) in EXPECTATIONS[Set]
    return Set{AbstractString}([decode_response(sock, cmd_called, true) for i = 1:len])
  elseif lowercase(cmd_called) in EXPECTATIONS[Array]
    return [decode_response(sock, cmd_called, true) for i = 1:len]
  elseif lowercase(cmd_called) in EXPECTATIONS[Libc.TmStruct]
    secs, mills = [decode_response(sock, cmd_called, true) for i = 1:len]
    return Libc.TmStruct(parse(Int,secs))
  else
    return [decode_response(sock, cmd_called, true) for i = 1:len]
#    throw(KeyError(EXPECTATIONS))
  end
end

function parse_unknown_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)
#  println("running command '$cmd_called' resp '$resp'")
  throw(KeyError(EXPECTATIONS))
end

function decode_response(sock::IO, cmd_called, sub_parse=false)
  resp = readuntil(sock, CRLF)
  resp = strip(resp, CRLFc)
#  println("running command '$cmd_called' resp '$resp'")
  if resp[1] == '+'
    return parse_string_response(sock, cmd_called, sub_parse, resp)
  elseif resp[1] == ':'
    return parse_int_response(sock, cmd_called, sub_parse, resp)
  elseif resp[1] == '-'
    return parse_error_response(sock, cmd_called, sub_parse, resp)
  elseif resp[1] == '$'
    return parse_bulk_string_response(sock, cmd_called, sub_parse, resp)
  elseif resp[1] == '*'
    return parse_array_response(sock, cmd_called, sub_parse, resp)
  else
    return parse_unknown_response(sock, cmd_called, sub_parse, resp)
  end
end

struct Connection
  socket_file::AbstractString
  host::IPAddr
  port::Int64
  password::AbstractString
  database::Int64

  socket::TCPSocket

  Connection() = new("", ip"127.0.0.1", 6379, "", 0)
end

function client(;socket::AbstractString="", host::IPAddr=ip"127.0.0.1", port::Int64=6379, password::AbstractString="", db::Int64=0)
  c = socket == "" ? connect(host, port) : connect(socket)
  password == "" || auth(c, password)
  db == 0 || select!(c, db)
  return c
end

function send(sock::TCPSocket, redis_cmd::AbstractString, args...)
#  @printf "sending '%s'\n" redis_type([redis_cmd, args...])
  write(sock, redis_type([redis_cmd, args...]))
  decode_response(sock, redis_cmd)
end
shutdown(sock::IO, save::Bool)                             = ( write(sock, redis_type(["SHUTDOWN", save ? "SAVE" : "NOSAVE"]));   readall(sock); close(sock);          "" )
shutdown(sock::IO)                                         = ( write(sock, redis_type(["SHUTDOWN"]));                             readall(sock); close(sock);          "" )
function shutdown(sock::IO, save::AbstractString)
  if lowercase(save) == "save"
    shutdown(sock, true)
  elseif lowercase(save) == "nosave"
    shutdown(sock, false)
  else
    shutdown(sock)
  end
end
#client_send(sock::TcpSocket,  redis_cmd::AbstractString, args...) = ( write(sock, redis_type([string("CLIENT ",  redis_cmd), args...])); decode_response(sock, redis_cmd) )
#cluster_send(sock::TcpSocket, redis_cmd::AbstractString, args...) = ( write(sock, redis_type([string("CLUSTER ", redis_cmd), args...])); decode_response(sock, redis_cmd) )
#command_send(sock::TcpSocket, redis_cmd::AbstractString, args...) = ( write(sock, redis_type([string("COMMAND ", redis_cmd), args...])); decode_response(sock, redis_cmd) )
#config_send(sock::TcpSocket,  redis_cmd::AbstractString, args...) = ( write(sock, redis_type([string("CONFIG ",  redis_cmd), args...])); decode_response(sock, redis_cmd) )
#debug_send(sock::TcpSocket,   redis_cmd::AbstractString, args...) = ( write(sock, redis_type([string("DEBUG ",   redis_cmd), args...])); decode_response(sock, redis_cmd) )
#script_send(sock::TcpSocket,  redis_cmd::AbstractString, args...) = ( write(sock, redis_type([string("SCRIPT ",  redis_cmd), args...])); decode_response(sock, redis_cmd) )

#!connection group
auth(sock::IO,          password::AbstractString)   =  send(sock, "AUTH",         password)
echo(sock::IO,          message::AbstractString)    =  send(sock, "ECHO",         message)
ping(sock::IO)                                      =  send(sock, "PING")             
#quit(sock::IO)                                     =  ( r = send(sock, "QUIT"); isopen(sock) ? (close(sock); r) : r )
function quit(sock::IO)
  r = send(sock, "QUIT");
  isopen(sock) && close(sock)
  r
end
# TODO: consider adding a select method which can run one specific command on the specified database
select!(sock::IO,       index::Int64)                                                                      =  send(sock, "SELECT",       string(index))
#TODO: client side current database; would need to change how we connect though, to keep track of this.
#current(sock::IO)                        =  ( sock.database )

#!generic group
del(sock::IO,           keys::AbstractString...)                                                                   =  send(sock, "DEL",          keys...)
#dump
#exists(sock::IO,        keys::AbstractString...)                                                                   =  send(sock, "EXISTS",       keys...)
function exists(client::IO,        keys::AbstractString...)
#  if client.server_major_version == 3.0 && client.server_minor_version >= 3
#    send(client, "EXISTS",       keys)
#  else
    sum([send(client, "EXISTS",  key) for key in keys])
#  end
end
expire(sock::IO,        key::AbstractString,                  by::Int64)                                           =  send(sock, "EXPIRE",       key,                string(by))
expireat(sock::IO,      key::AbstractString,                  when::Int64)                                         =  send(sock, "EXPIREAT",     key,                string(when))
keys(sock::IO,          matching::AbstractString="*")                                                              =  send(sock, "KEYS",         matching)
#migrate
move(sock::IO,          key::AbstractString,                  index::Int64)                                        =  send(sock, "MOVE",         key,                string(index))
#object
persist(sock::IO,       key::AbstractString)                                                                       =  send(sock, "PERSIST",      key)
pexpire(sock::IO,       key::AbstractString,                  when::Int64)                                         =  send(sock, "PEXPIRE",      key,                string(when))
pexpireat(sock::IO,     key::AbstractString,                  when::Int64)                                         =  send(send, "PEXPIREAT",    key,                string(when))
pttl(sock::IO,          key::AbstractString)                                                                       =  send(sock, "PTTL",         key)
randomkey(sock::IO)                                                                                        =  send(sock, "RANDOMKEY")
rename(sock::IO,        key::AbstractString,                  newkey::AbstractString;     not_exists=false)                =  send(sock, "RENAME",       key,                 newkey)
renamenx(sock::IO,      key::AbstractString,                  newkey::AbstractString)                                      =  rename(sock, key,          newkey,              not_exists=true)
#restore
#scan
#sort
ttl(sock::IO,           key::AbstractString)                                                                       =  send(sock, "TTL",          key)
typeof(sock::IO,        key::AbstractString)                                                                       =  send(sock, "TYPE",         key)
#wait

#!transactions group
#discard
#exec
#multi
#unwatch
#watch

#!string group
append(sock::IO,        key::AbstractString,                  val::AbstractString)                                        = send(sock,        "APPEND",           key,             val)

bitcount(sock::IO,      key::AbstractString,                  start::Int64=0,     nd::Int64=-1)                   = send(sock,        "BITCOUNT",         key,             string(start), string(nd))
bitop(sock::IO,         op::AbstractString,                   destkey::AbstractString,    key::AbstractString...)                 = send(sock,        "BITOP",            op,              destkey,       key...)
bitpos(sock::IO,        key::AbstractString,                  bit::Int,           start::Int64=0,  nd::Int64=-1)  = send(sock,        "BITPOS",           key,             string(bit),   string(start),  string(nd))
getbit(sock::IO,        key::AbstractString,                  bit::Int)                                           = send(sock,        "GETBIT",           key,             string(bit))
setbit(sock::IO,        key::AbstractString,                  bit::Int,           value::Any)                     = send(sock,        "SETBIT",           key,             string(bit),   string(value))

decr(sock::IO,          key::AbstractString)                                                                      = send(sock,        "DECR",             key)
decrby(sock::IO,        key::AbstractString,                  by::Int64)                                          = send(sock,        "DECRBY",           key,             string(by))
decrby(sock::IO,        key::AbstractString,                  by::Float64)                                        = incrbyfloat(sock, key,                -1by)
decrbyfloat(sock::IO,   key::AbstractString,                  by::Float64)                                        = incrbyfloat(sock, key,                -1by)
incr(sock::IO,          key::AbstractString)                                                                      = send(sock,        "INCR",             key)
incrby(sock::IO,        key::AbstractString,                  by::Float64)                                        = send(sock,        "INCRBYFLOAT",      key,             string(by))
incrby(sock::IO,        key::AbstractString,                  by::Int64)                                          = send(sock,        "INCRBY",           key,             string(by))
incrbyfloat(sock::IO,   key::AbstractString,                  by::Real)                                           = send(sock,        "INCRBYFLOAT",      key,             string(by))

getrange(sock::IO,      key::AbstractString,                  start::Int64,       nd::Int64)                      = send(sock,        "GETRANGE",         key,             string(start), string(nd))
setrange(sock::IO,      key::AbstractString,                  start::Int64,       value::Any)                     = send(sock,        "SETRANGE",         key,             string(start),        string(value))
getset(sock::IO,        key::AbstractString,                  val::AbstractString)                                        = send(sock,        "GETSET",           key,             val)

get(sock::IO,           key::AbstractString)                                                                      = send(sock,        "GET",              key)
get(sock::IO,           key::Char)                                                                        = get(sock,         string(key))
get(sock::IO,           key::Int64)                                                                       = get(sock,         string(key))


mget(sock::IO,          keys::Array;                  sorted=false)                                       = ( r =      send(sock, "MGET",   map(string, keys));        sorted ? sort(r) : r )
mget(sock::IO,          keys::Any...;                 sorted=false)                                       = mget(sock, map(string,  keys),          sorted=sorted)
#mget(sock::IO,          keys::Array;                  sorted=false)                                       = mget(sock, map(string,  keys),          sorted=sorted)
get(sock::IO,           keys::Any...;                 sorted=false)                                       = mget(sock, keys...,             sorted=sorted)
get(sock::IO,           keys::Array;                  sorted=false)                                       = mget(sock, keys...,             sorted=sorted)

mset(sock::IO,          key_val::Array;               not_exists=false)                                   = send(sock, not_exists ?         "MSETNX" :       "MSET",    collect(map(string, key_val)))
#mset(sock::IO,          key_val::Array;               not_exists=false)                                   = send(sock, not_exists ?         "MSETNX" :       "MSET",    collect(map(string, key_val))...)
#mset(sock::IO,          key_val::Array;               not_exists=false)                                   = mset(sock, map(string,  key_val),       not_exists=not_exists)
mset(sock::IO,          keys::Any...;                 not_exists=false)                                   = mset(sock, keys,          not_exists=not_exists)
msetnx(sock::IO,        key_val::Array)                                                                   = mset(sock, key_val,       not_exists=true)
msetnx(sock::IO,        keys::Any...)                                                                     = mset(sock, keys,          not_exists=true)

function set(sock::IO, key::AbstractString, value::Any; sec_expire::Int=-1, ms_expire::Int=-1, not_exists::Bool=false, if_exists::Bool=false)
  cmd_msg = AbstractString["SET", key, value]
  sec_expire > -1 && push!(cmd_msg, "EX", string(sec_expire))
  ms_expire > -1 && push!(cmd_msg, "PX", string(ms_expire))
  if not_exists && if_exists
    throw("not_exists and if_exists cannot be set simultaneously")
    return 0
  end
  not_exists && push!(cmd_msg, "NX")
  if_exists && push!(cmd_msg, "XX")

  send(sock, cmd_msg...)
end

function set(sock::IO,  keys::Array)
  if length(keys) % 2 == 1
    warn("Appending empty value to uneven array.")
    push!(keys, "")
  end
  mset(sock, map(string, keys)...)
end

set(sock::IO,     key::AbstractString,      val::Char)                     = set(sock,  key,               string(val))
set(sock::IO,     key::AbstractString,      val::Int64)                    = set(sock,  key,               string(val))
set(sock::IO,     key_val::Any...)                                 = set(sock,  map(string, key_val))
setex(sock::IO,   key::AbstractString,      seconds::Int,      value::Any) = set(sock,  key,               value,        sec_expire=seconds)
setnx(sock::IO,   key::AbstractString,      value::Any)                    = set(sock,  key,               value,        not_exists=true)
psetex(sock::IO,  key::AbstractString,      milliseconds::Int, value::Any) = set(sock,  key,               value,        ms_expire=milliseconds)
strlen(sock::IO,  key::AbstractString)                                     = send(sock, "STRLEN",          key)
#!end string group

#!hash group
hdel(sock::IO,         key::AbstractString,  hkeys::AbstractString...)                                           = send(sock,         "HDEL",         key,   hkeys...)
hexists(sock::IO,      key::AbstractString,  field::Any)                                                 = send(sock,         "HEXISTS",      key,   field)
hget(sock::IO,         key::AbstractString,  field::AbstractString)                                              = send(sock,         "HGET",         key,   field)
hget(sock::IO,         key::AbstractString,  fields::Array)                                              = hmget(sock,        key,            fields)
hgetall(sock::IO,      key::AbstractString)                                                              = send(sock,         "HGETALL",      key)

hdecr(sock::IO,        key::AbstractString,  field::AbstractString)                                              = hincrby(sock,      key,            field, -1)
hdecrby(sock::IO,      key::AbstractString,  field::AbstractString,     by::Int64)                               = send(sock,         "HINCRBY",      key,   field,        string(-1by))
hdecrby(sock::IO,      key::AbstractString,  field::AbstractString,     by::Float64)                             = hincrbyfloat(sock, key,            field, -1by)
hdecrbyfloat(sock::IO, key::AbstractString,  field::AbstractString,     by::Real)                                = send(sock,         "HINCRBYFLOAT", key,   field,        string(-1by))
hincr(sock::IO,        key::AbstractString,  field::AbstractString)                                              = hincrby(sock,      key,            field, 1)
hincrby(sock::IO,      key::AbstractString,  field::AbstractString,     by::Int64)                               = send(sock,         "HINCRBY",      key,   field,        string(by))
hincrby(sock::IO,      key::AbstractString,  field::AbstractString,     by::Float64)                             = hincrbyfloat(sock, key,            field, by)
hincrbyfloat(sock::IO, key::AbstractString,  field::AbstractString,     by::Real)                                = send(sock,         "HINCRBYFLOAT", key,   field,        string(by))

hkeys(sock::IO,        key::AbstractString;  sorted=false)                                               = ( r = send(sock,   "HKEYS",        key);  sorted ?      sort(r) : r )
hlen(sock::IO,         key::AbstractString)                                                              = send(sock,         "HLEN",         key)
#hmget(sock::IO,        key::AbstractString,  fields::Array{AbstractString})                                      = send(sock,         "HMGET",        key,                 fields...)
hmget(sock::IO,        key::AbstractString,  fields::Array)                                              = send(sock,         "HMGET",        key,                 map(string, fields)...)
hmget(sock::IO,        key::AbstractString,  fields::Any...)                                             = hmget(sock,        key,            map(string,  fields))
hmset(sock::IO,        key::AbstractString,  field_vals::Array)                                          = send(sock,         "HMSET",        key,            map(string,  field_vals)...)
hmset(sock::IO,        key::AbstractString,  field_vals::Any...)                                         = hmset(sock,        key,            map(string,  field_vals))
#hscan
hset(sock::IO,         key::AbstractString,  field::AbstractString,     value::AbstractString;  not_exists::Bool=false)  = send(sock,         not_exists ? "HSETNX" : "HSET",       key,   field,    value)
hset(sock::IO,         key::AbstractString,  field::AbstractString,     value::Any;     not_exists::Bool=false)  = hset(sock,         key,            field, string(value), not_exists=not_exists)
hsetnx(sock::IO,       key::AbstractString,  field::AbstractString,     value::AbstractString)                           = hset(sock,         key,            field, value,         not_exists=true)
#hstrlen(sock::IO,  key::AbstractString,  field::AbstractString)                                                  = send(sock,        "HSTRLEN",  key,   field)
function hstrlen(sock::IO,  key::AbstractString,  field::AbstractString)
#  if client.server_major_version >= 3.2 && client.server_minor_version >= 0
#    send(sock,        "HSTRLEN",  key,   field)
#  else
    length(hget(sock, key, field))
#  end
end
hvals(sock::IO,        key::AbstractString; sorted=true)                                                 = ( r = send(sock,  "HVALS",    key);  sorted ? sort(r) : r )
#!end hashes

#!sets
sadd(sock::IO,          key::AbstractString,                 smems::Any...)                                             =  send(sock, "SADD",         key,         smems...)
scard(sock::IO,         key::AbstractString)                                                                            =  send(sock, "SCARD",        key)
sdiff(sock::IO,         key::AbstractString,                 keys::AbstractString...)                                           =  send(sock, "SDIFF",        key,         keys...)
sdiffstore(sock::IO,    destination::AbstractString,         key::AbstractString,          keys::AbstractString...)                     =  send(sock, "SDIFFSTORE",   destination, key,                     keys...)
sinter(sock::IO,        key::AbstractString,                 keys::AbstractString...)                                           =  send(sock, "SINTER",       key,         keys...)
sinterstore(sock::IO,   destination::AbstractString,         key::AbstractString,          keys::AbstractString...)                     =  send(sock, "SINTERSTORE",  destination, key,                     keys...)
sismember(sock::IO,     key::AbstractString,                 smem::Any)                                                 =  send(sock, "SISMEMBER",    key,         smem)
smembers(sock::IO,      key::AbstractString)                                                                            =  send(sock, "SMEMBERS",     key)
smove(sock::IO,         source::AbstractString,              destination::AbstractString,  member::Any)                         =  send(sock, "SMOVE",        source,      destination,             member)
spop(sock::IO,          key::AbstractString)                                                                            =  send(sock, "SPOP",         key)
spop(sock::IO,          key::AbstractString,                 count::Int64)                                              = ( Set([ send(sock, "SPOP",  key)     for i = 1:count ]) )
#spop(sock::IO,          key::AbstractString,                 count::Int64)                                              =  send(sock, "SPOP",         key,         string(count))
# Because, according to the documentation, this feature hasn't yet been implemented server side
# we'll fake it for now so our tests work.
srandmember(sock::IO,   key::AbstractString)                                                                            =  send(sock, "SRANDMEMBER",  key)
srandmember(sock::IO,   key::AbstractString,                 count::Int64)                                              =  send(sock, "SRANDMEMBER",  key,         string(count))
srem(sock::IO,          key::AbstractString,                 members::AbstractString...)                                        =  send(sock, "SREM",         key,         members...)
#sscan(sock::IO,        key::AbstractString,                 cursor::Int64;        matching::Regex="",  count::Int64=-1) =  send(sock, "SSCAN",        key,         cursor,      matching,  count)
sunion(sock::IO,        key::AbstractString,                 members::Any...)                                           =  send(sock, "SUNION",       key,         members...)
sunionstore(sock::IO,   destination::AbstractString,         key::AbstractString,          keys::AbstractString...)                     =  send(sock, "SUNIONSTORE",  destination, key,                     keys...)
#!end sets

#!hyperloglogs
#pfadd
#pfcount
#pfmerge

#!pubsub
#psubscribe
#publish
#pubsub
#punsubscribe
#subscribe
#unsubscribe

#!sorted sets
function zadd(sock::IO, key::AbstractString, score_members::Array; not_exists::Bool=false, if_exists::Bool=false, changes::Bool=false, incr::Bool=false)
  options = AbstractString[]
  if if_exists && not_exists
    throw("Cannot simultaneously specify if_exists and not_exists.")
    return 0
  end
  not_exists == true && push!(options, "NX")
  if_exists  == true && push!(options, "XX")
  changes    == true && push!(options, "CH")
  incr       == true && push!(options, "INCR")
  if length(options) > 0
    send(sock, "ZADD", key, options..., map(string, score_members)...)
  else
    send(sock, "ZADD", key, map(string, score_members)...)
  end
end
zadd(sock::IO,     key::AbstractString,                                                score_members::Any...;  not_exists::Bool=false, if_exists::Bool=false, changes::Bool=false, incr::Bool=false) = zadd(sock, key, map(string, score_members), not_exists=not_exists,       if_exists=if_exists,        changes=changes,           incr=incr)
zadd(sock::IO,     key::AbstractString, exists::AbstractString, changes::AbstractString, incr::AbstractString, score_members::Array)  = zadd(sock, key,      map(string, score_members), not_exists=(exists == "NX"),  if_exists=(exists == "XX"),  changes=(changes == "CH"), incr=(incr == "INCR"))
zadd(sock::IO,     key::AbstractString, exists::AbstractString, changes::AbstractString, incr::AbstractString, score_members::Any...) = zadd(sock, key,      map(string, score_members), not_exists=(exists == "NX"),  if_exists=(exists == "XX"),  changes=(changes == "CH"), incr=(incr == "INCR"))
zadd(sock::IO,     key::AbstractString, exists::AbstractString, chinc::AbstractString,                 score_members::Array)  = zadd(sock, key,      map(string, score_members), not_exists=(exists == "NX"),  if_exists=(exists == "XX"),  changes=(chinc == "CH"),   incr=(chinc == "INCR"))
zadd(sock::IO,     key::AbstractString, exists::AbstractString, chinc::AbstractString,                 score_members::Any...) = zadd(sock, key,      map(string, score_members), not_exists=(exists == "NX"),  if_exists=(exists == "XX"),  changes=(chinc == "CH"),   incr=(chinc == "INCR"))
zadd(sock::IO,     key::AbstractString, exchinc::AbstractString,                               score_members::Array)  = zadd(sock, key,      map(string, score_members), not_exists=(exchinc == "NX"), if_exists=(exchinc == "XX"), changes=(exchinc == "CH"), incr=(exchinc == "INCR"))
zadd(sock::IO,     key::AbstractString, exchinc::AbstractString,                               score_members::Any...) = zadd(sock, key,      map(string, score_members), not_exists=(exchinc == "NX"), if_exists=(exchinc == "XX"), changes=(exchinc == "CH"), incr=(exchinc == "INCR"))
zcard(sock::IO,    key::AbstractString)                                                                       = send(sock, "ZCARD",  key)
zcount(sock::IO,   key::AbstractString, min::Number,    max::Number)                                          = send(sock, "ZCOUNT", key,                string(1.0min),  string(1.0max))
#zincrby
#zinterstore
#zlexcount
#zrange
#zrangebylex
#zrangebyscore
zrank(sock::IO,    key::AbstractString,  member::AbstractString)                                                           = send(sock,  "ZRANK",   key,  member)
#zrem
#zremrangebylex
#zremrangebyrank
#zremrangebyscore
#zrevrange
#zrevrangebylex
#zrevrangebyscore
#zrevrank
#zscan
zscore(sock::IO,   key::AbstractString,  member::AbstractString)                                                           = send(sock,  "ZSCORE",  key,  member)
#zunionstore
#!end sorted sets

#!lists
#blpop
#brpop
#brpoplpush
lindex(sock::IO, key::AbstractString, index::Int64)                                              = send(sock,  "LINDEX",    key,       string(index))
struct InvalidInsertionMethodException <: Exception
  retail::AbstractString
end
function linsert(sock::IO, key::AbstractString, how::AbstractString, where::AbstractString, value::AbstractString)
  if lowercase(how) in ["before", "after"]
    send(sock, "LINSERT", key, how, where, value)
  else
    throw(InvalidInsertionMethodException("Unknown insertion method $how"))
  end
end
llen(sock::IO,       key::AbstractString)                                                        = send(sock,   "LLEN",      key)
lpop(sock::IO,       key::AbstractString)                                                        = send(sock,   "LPOP",      key)

function lpush(sock::IO, key::AbstractString, values::Array; if_exists::Bool=false)
  if if_exists
    res = send(sock, "LPUSHX", key, shift!(values))
    res == 0 && return 0
    length(values) == 0 && return res
  end
  send(sock,   "LPUSH", key, map(string, values)...)
end
#lpush(sock::IO,     key::AbstractString,   values::Array{AbstractString};       if_exists::Bool=false)  = send(sock,   if_exists ?  "LPUSHX" : "LPUSH",           key,                 values...)
lpush(sock::IO,      key::AbstractString,   value::AbstractString;               if_exists::Bool=false)  = lpush(sock,  key,         [value],                      if_exists=if_exists)
#lpush(sock::IO,     key::AbstractString,   values::Array;               if_exists::Bool=false)  = lpush(sock,  key,         map(string, values), if_exists=if_exists)
lpush(sock::IO,      key::AbstractString,   values::Any...;              if_exists::Bool=false)  = lpush(sock,  key,         map(string, values), if_exists=if_exists)

lpushx(sock::IO,     key::AbstractString,   value::AbstractString)                                       = lpush(sock,  key,         value,     if_exists=true)

lrange(sock::IO,     key::AbstractString,   start::Int64,                stop::Int64)            = send(sock,   "LRANGE",    key,       string(start),     string(stop))
lrem(sock::IO,       key::AbstractString,   count::Int64,                value::AbstractString)          = send(sock,   "LREM",      key,       string(count),     value)
lset(sock::IO,       key::AbstractString,   index::Int64,                value::AbstractString)          = send(sock,   "LSET",      key,       string(index),     value)
ltrim(sock::IO,      key::AbstractString,   start::Int64,                stop::Int64)            = send(sock,   "LTRIM",     key,       string(start),     string(stop))
rpop(sock::IO,       key::AbstractString)                                                        = send(sock,   "RPOP",      key)
rpoplpush(sock::IO,  skey::AbstractString,  dkey::AbstractString)                                        = send(sock,   "RPOPLPUSH", skey,      dkey)

function rpush(sock::IO, key::AbstractString, values::Array; if_exists::Bool=false)
  if if_exists
    res = send(sock, "RPUSHX", key, shift!(values))
    res == 0 && return 0
    length(values) == 0 && return res
  end
  send(sock,   "RPUSH", key, map(string, values)...)
end
#rpush(sock::IO,     key::AbstractString,   values::Array{AbstractString};       if_exists::Bool=false)  = send(sock,   if_exists ?  "RPUSHX" : "RPUSH", key,      values...)
rpush(sock::IO,      key::AbstractString,   value::AbstractString;               if_exists::Bool=false)  = rpush(sock,  key,         [value],                      if_exists=if_exists)
#rpush(sock::IO,     key::AbstractString,   values::Array;               if_exists::Bool=false)  = rpush(sock,  key,         map(string, values), if_exists=if_exists)
rpush(sock::IO,      key::AbstractString,   values::Any...;              if_exists::Bool=false)  = rpush(sock,  key,         map(string, values), if_exists=if_exists)

rpushx(sock::IO,     key::AbstractString,   value::AbstractString)                               = rpush(sock,  key,         value,     if_exists=true)

# client only commands (w/ some help from the core method definitions)
lpop(sock::IO,       key::AbstractString,   count::Int64)                                        = [lpop(sock,  key) for i = 1:count]
lrange(sock::IO,     key::AbstractString)                                                        = lrange(sock, key, 0, -1)
rpop(sock::IO,       key::AbstractString,   count::Int64)                                        = [rpop(sock,  key) for i = 1:count]

#lpushx(sock::IO,    key::AbstractString,   values::Array{AbstractString})                       = lpush(sock,  key,         values,                       if_exists=true)
#lpushx(sock::IO,    key::AbstractString,   values::Array)                                       = lpush(sock,  key,         map(string, values), if_exists=true)
lpushx(sock::IO,     key::AbstractString,   values::Any...)                                      = lpush(sock,  key,         map(string, values), if_exists=true)
#
#rpushx(sock::IO,    key::AbstractString,   values::Array{AbstractString})                       = rpush(sock,  key,         values,                       if_exists=true)
#rpushx(sock::IO,    key::AbstractString,   values::Array)                                       = rpush(sock,  key,         map(string, values), if_exists=true)
rpushx(sock::IO,     key::AbstractString,   values::Any...)                                      = rpush(sock,  key,         map(string, values), if_exists=true)
#!end lists

#!scripting group
#script exists
#script flush
#script kill
#script load
#eval
#evalsha

#!cluster group
#cluster addslots
#cluster count-failure-reports
#cluster countkeysinslot
#cluster delslots
#cluster failover
#cluster forget
#cluster getkeysinslot
#cluster info
#cluster keyslot
#cluster meet
#cluster nodes
#cluster replicate
#cluster reset
#cluster saveconfig
#cluster set-config-epoch
#cluster setslot
#cluster slaves
#cluster slots

#!server group
bgrewriteaof(sock::IO)                    =  send(sock, "BGREWRITEAOF")
bgsave(sock::IO)                          =  save(sock, background=true)
#client getname
#client kill
#client list
#client pause
#client setname
#command
#command count
#command getkeys
#command info
#config get
#config resetstat
#config rewrite
#config set
dbsize(sock::IO)                          =  send(sock, "DBSIZE")
#function dbsize(sock::IO, index::Int64) 
#  cdb = current(sock)
#  select!(sock, index)
#  r = send(sock, "DBSIZE")
#  select!(sock, cdb)
#  r
#end
#debug object
#debug segfault
flushall(sock::IO)                        =  send(sock, "FLUSHALL")
flushdb(sock::IO)                         =  send(sock, "FLUSHDB")
function info(sock::IO, section::AbstractString="default")
  if lowercase(section) == "default"
    send(sock, "INFO")
  elseif lowercase(section) in [ "all", "clients", "cluster", "commandstats", "cpu", "keyspace", "memory", "persistence", "replication", "server", "stats" ]
    send(sock, "INFO", section)
  else
    throw("Invalid section '$section'")
  end
end
lastsave(sock::IO)                        =  send(sock, "LASTSAVE")
#monitor
#role
save(sock::IO;          background=false) =  send(sock, background ? "BGSAVE" : "SAVE")
#slaveof
#slowlog
#sync
time(sock::IO)                            =  send(sock, "TIME")

#! geo group
#geoadd
#geodist
#geohash
#geopos
#georadius
#georadiusbymember
end # module
