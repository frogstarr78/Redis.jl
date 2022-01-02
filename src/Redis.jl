module Redis

import Base: TCPSocket, IPAddr
import Base
export client

CRLFc = ['\r', '\n']
CRLF = join(CRLFc)
EXPECTATIONS = Dict{Type,Set}(
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
	AbstractFloat => Set([
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

redis_type(arg::Void)                           = string('$', -1, CRLF)
redis_type(arg::AbstractString; simple::Bool=false) = (simple ? string('+', arg, CRLF ) : string('$', join([length(arg), arg], CRLF), CRLF))
#redis_type(arg::Union)                              = string('$', -1, CRLF)
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

type UnAuthException <: Exception end
type ErrorException <: Exception
	detail::AbstractString
end
type InvalidSectionException <: Exception
	section::String
end
Base.showerror(io::IO, e::InvalidSectionException) = print(io, e.section. "Invalid section")

parse_error_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)   = throw(ErrorException(resp[2:end]))
function parse_string_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)
	if lowercase(cmd_called) in EXPECTATIONS[AbstractString] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
		return resp[2:end]
	else
#		@printf "+cmd_called '%s'\n" cmd_called
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
#		@printf ":cmd_called '%s'\n" cmd_called
		throw(KeyError(EXPECTATIONS))
	end
end

function parse_bulk_string_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)
#	@printf "cmd_called '%s', sub_parse '%s', in EXPECTATIONS '%s'\n" cmd_called sub_parse ( lowercase(cmd_called) in EXPECTATIONS[AbstractString] || lowercase(cmd_called) in EXPECTATIONS[Set] || ( sub_parse && ( lowercase(cmd_called) in EXPECTATIONS[Array] || lowercase(cmd_called) in EXPECTATIONS[Dict] ) ) )
#	@printf "cmd_called '%s', sub_parse '%s', in EXPECTATIONS '%s'\n" cmd_called sub_parse ( lowercase(cmd_called) in EXPECTATIONS[AbstractFloat] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] ) )
#	@printf "cmd_called '%s', sub_parse '%s', in EXPECTATIONS '%s'\n" cmd_called sub_parse ( lowercase(cmd_called) in EXPECTATIONS[Libc.TmStruct] && sub_parse )
	if lowercase(cmd_called) in EXPECTATIONS[AbstractString] || lowercase(cmd_called) in EXPECTATIONS[Set] || ( sub_parse && ( lowercase(cmd_called) in EXPECTATIONS[Array] || lowercase(cmd_called) in EXPECTATIONS[Dict] ) )
		len = parse(Int,resp[2:end])
		if len == -1
			return nothing
		else
			r = join(map(Char, read(sock, len)))
			readline(sock) #emptying the io buffer
			return r
		end
	elseif lowercase(cmd_called) in EXPECTATIONS[AbstractFloat] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
		len = parse(Int,resp[2:end])
		r = join(map(Char, read(sock, len)))
		readline(sock) #emptying the io buffer
		return parse(Float64,r)
	elseif lowercase(cmd_called) in EXPECTATIONS[Libc.TmStruct] && sub_parse
		len = parse(Int,resp[2:end])
		r = join(map(Char, read(sock, len)))
		readline(sock) #emptying the io buffer
		return r
	else
#		@printf "\$cmd_called '%s'\n" cmd_called
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
#		throw(KeyError(EXPECTATIONS))
	end
end

function parse_unknown_response(sock::IO, cmd_called, sub_parse, resp::AbstractString)
#	println("running command '$cmd_called' resp '$resp'")
	throw(KeyError(EXPECTATIONS))
end

function decode_response(sock::IO, cmd_called, sub_parse=false)
	resp = readline(sock, chomp=true)
#	resp = strip(resp, CRLFc)
#	println("running command '$cmd_called' resp '$resp'")
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

immutable Connection
	socket_file::AbstractString
	host::IPAddr
	port::Integer
	password::AbstractString
	database::Integer

	socket::TCPSocket

	Connection() = new("", ip"127.0.0.1", 6379, "", 0)
end

function client(;socket::AbstractString="", host::IPAddr=ip"127.0.0.1", port::Integer=6379, password::AbstractString="", db::Integer=0)
	c = socket == "" ? connect(host, port) : connect(socket)
	password == "" || auth(c, password)
	db == 0 || select!(c, db)
	return c
end

function send(sock::TCPSocket, redis_cmd::AbstractString, args...)
#	@printf "sending '%s'\n" redis_type([redis_cmd, args...])
	write(sock, redis_type([redis_cmd, args...]))
	decode_response(sock, redis_cmd)
end
shutdown(sock::IO, save::Bool)                             = ( write(sock, redis_type(["SHUTDOWN", save ? "SAVE" : "NOSAVE"]));   read(sock); close(sock);          "" )
shutdown(sock::IO)                                         = ( write(sock, redis_type(["SHUTDOWN"]));                             read(sock); close(sock);          "" )
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
auth(sock::IO,          password::AbstractString)                                                                  =  send(sock, "AUTH",         password)
echo(sock::IO,          message::AbstractString)                                                                   =  send(sock, "ECHO",         message)
ping(sock::IO)                                                                                             =  send(sock, "PING")             
#quit(sock::IO)                                                                                             =  ( r = send(sock, "QUIT"); isopen(sock) ? (close(sock); r) : r )
function quit(sock::IO)
	r = send(sock, "QUIT");
	isopen(sock) && close(sock)
	r
end
# TODO: consider adding a select method which can run one specific command on the specified database
select!(sock::IO,       index::Integer)                                                                      =  send(sock, "SELECT",       string(index))
#TODO: client side current database; would need to change how we connect though, to keep track of this.
#current(sock::IO)                        =  ( sock.database )

#!generic group
del(sock::IO,           keys::AbstractString...)                                                                   =  send(sock, "DEL",          keys...)
#dump
#exists(sock::IO,        keys::AbstractString...)                                                                   =  send(sock, "EXISTS",       keys...)
function exists(client::IO,        keys::AbstractString...)
#	if client.server_major_version == 3.0 && client.server_minor_version >= 3
#		send(client, "EXISTS",       keys)
#	else
		sum([send(client, "EXISTS",  key) for key in keys])
#	end
end
expire(sock::IO,        key::AbstractString,                  by::Integer)                                           =  send(sock, "EXPIRE",       key,                string(by))
expireat(sock::IO,      key::AbstractString,                  when::Integer)                                         =  send(sock, "EXPIREAT",     key,                string(when))
keys(sock::IO,          matching::AbstractString="*")                                                              =  send(sock, "KEYS",         matching)
#migrate
move(sock::IO,          key::AbstractString,                  index::Integer)                                        =  send(sock, "MOVE",         key,                string(index))
#object
persist(sock::IO,       key::AbstractString)                                                                       =  send(sock, "PERSIST",      key)
pexpire(sock::IO,       key::AbstractString,                  when::Integer)                                         =  send(sock, "PEXPIRE",      key,                string(when))
pexpireat(sock::IO,     key::AbstractString,                  when::Integer)                                         =  send(send, "PEXPIREAT",    key,                string(when))
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

bitcount(sock::IO,      key::AbstractString,                  start::Integer=0,     nd::Integer=-1)                   = send(sock,        "BITCOUNT",         key,             string(start), string(nd))
bitop(sock::IO,         op::AbstractString,                   destkey::AbstractString,    key::AbstractString...)                 = send(sock,        "BITOP",            op,              destkey,       key...)
bitpos(sock::IO,        key::AbstractString,                  bit::Int,           start::Integer=0,  nd::Integer=-1)  = send(sock,        "BITPOS",           key,             string(bit),   string(start),  string(nd))
getbit(sock::IO,        key::AbstractString,                  bit::Int)                                           = send(sock,        "GETBIT",           key,             string(bit))
setbit(sock::IO,        key::AbstractString,                  bit::Int,           value::Any)                     = send(sock,        "SETBIT",           key,             string(bit),   string(value))

decr(sock::IO,          key::AbstractString)                                                                      = send(sock,        "DECR",             key)
decrby(sock::IO,        key::AbstractString,                  by::Integer)                                          = send(sock,        "DECRBY",           key,             string(by))
decrby(sock::IO,        key::AbstractString,                  by::AbstractFloat)                                        = incrbyfloat(sock, key,                -1by)
decrbyfloat(sock::IO,   key::AbstractString,                  by::AbstractFloat)                                        = incrbyfloat(sock, key,                -1by)
incr(sock::IO,          key::AbstractString)                                                                      = send(sock,        "INCR",             key)
incrby(sock::IO,        key::AbstractString,                  by::AbstractFloat)                                        = send(sock,        "INCRBYFLOAT",      key,             string(by))
incrby(sock::IO,        key::AbstractString,                  by::Integer)                                          = send(sock,        "INCRBY",           key,             string(by))
incrbyfloat(sock::IO,   key::AbstractString,                  by::Real)                                           = send(sock,        "INCRBYFLOAT",      key,             string(by))

getrange(sock::IO,      key::AbstractString,                  start::Integer,       nd::Integer)                      = send(sock,        "GETRANGE",         key,             string(start), string(nd))
setrange(sock::IO,      key::AbstractString,                  start::Integer,       value::Any)                     = send(sock,        "SETRANGE",         key,             string(start),        string(value))
getset(sock::IO,        key::AbstractString,                  val::AbstractString)                                        = send(sock,        "GETSET",           key,             val)

get(sock::IO,           key::AbstractString)                                                                      = send(sock,        "GET",              key)
get(sock::IO,           key::Char)                                                                        = get(sock,         string(key))
get(sock::IO,           key::Integer)                                                                       = get(sock,         string(key))


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
set(sock::IO,     key::AbstractString,      val::Integer)                    = set(sock,  key,               string(val))
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
hdecrby(sock::IO,      key::AbstractString,  field::AbstractString,     by::Integer)                               = send(sock,         "HINCRBY",      key,   field,        string(-1by))
hdecrby(sock::IO,      key::AbstractString,  field::AbstractString,     by::AbstractFloat)                             = hincrbyfloat(sock, key,            field, -1by)
hdecrbyfloat(sock::IO, key::AbstractString,  field::AbstractString,     by::Real)                                = send(sock,         "HINCRBYFLOAT", key,   field,        string(-1by))
hincr(sock::IO,        key::AbstractString,  field::AbstractString)                                              = hincrby(sock,      key,            field, 1)
hincrby(sock::IO,      key::AbstractString,  field::AbstractString,     by::Integer)                               = send(sock,         "HINCRBY",      key,   field,        string(by))
hincrby(sock::IO,      key::AbstractString,  field::AbstractString,     by::AbstractFloat)                             = hincrbyfloat(sock, key,            field, by)
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
#	if client.server_major_version >= 3.2 && client.server_minor_version >= 0
#		send(sock,        "HSTRLEN",  key,   field)
#	else
		length(hget(sock, key, field))
#	end
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
spop(sock::IO,          key::AbstractString,                 count::Integer)                                              = ( Set([ send(sock, "SPOP",  key)     for i = 1:count ]) )
#spop(sock::IO,          key::AbstractString,                 count::Integer)                                              =  send(sock, "SPOP",         key,         string(count))
# Because, according to the documentation, this feature hasn't yet been implemented server side
# we'll fake it for now so our tests work.
srandmember(sock::IO,   key::AbstractString)                                                                            =  send(sock, "SRANDMEMBER",  key)
srandmember(sock::IO,   key::AbstractString,                 count::Integer)                                              =  send(sock, "SRANDMEMBER",  key,         string(count))
srem(sock::IO,          key::AbstractString,                 members::AbstractString...)                                        =  send(sock, "SREM",         key,         members...)
#sscan(sock::IO,        key::AbstractString,                 cursor::Integer;        matching::Regex="",  count::Integer=-1) =  send(sock, "SSCAN",        key,         cursor,      matching,  count)
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
lindex(sock::IO, key::AbstractString, index::Integer)                                              = send(sock,  "LINDEX",    key,       string(index))
type InvalidInsertionMethodException <: Exception
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

function cmd_push_dir(c::String,   exists=false)
	if c in ["l", "left", "front", "head"] 
		return exists ? "LPUSHX" : "LPUSH"
	elseif c in ["r", "right", "end"]
		return exists ? "RPUSHX" : "RPUSH"
	end
end

function cmd_push_dir(c::Char,     exists=false)
	if c in ['l', 'f', 'h']
		return exists ? "LPUSHX" : "LPUSH"
	elseif c in ['r', 'e']
		return exists ? "RPUSHX" : "RPUSH"
	end
end

function cmd_push_dir(c::Symbol, exists=false)
	if c in [:left, :front, :head]
		return exists ? "LPUSHX" : "LPUSH"
	elseif c in [:right, :end]
		return exists ? "RPUSHX" : "RPUSH"
	end
end

function push(sock::IO, key::AbstractString, values::Any...; dir=:left, if_exists::Bool=false)
	cmd = cmd_push_dir(isa(dir, Symbol) ? dir : lowercase(dir), if_exists)
	if if_exists
		res = map(value -> send(sock, cmd, key, string(value)), values)
		return isa(res, AbstractArray) ? sort(res)[end] : isa(res, Tuple) ? res[end] : res
	else
		send(sock, cmd, key, map(string, values)...)
	end
end

lpush(sock::IO,      key::AbstractString,   values::AbstractArray; if_exists=false)              = push(sock, key, values..., dir=:left, if_exists=if_exists);
lpush(sock::IO,      key::AbstractString,   values::Any...;        if_exists=false)              = push(sock, key, values..., dir=:left, if_exists=if_exists);
lpushx(sock::IO,     key::AbstractString,   values::Any...)                                      = push(sock, key, values..., dir=:left, if_exists=true);

rpush(sock::IO,      key::AbstractString,   values::AbstractArray; if_exists=false)              = push(sock, key, values..., dir=:right, if_exists=if_exists);
rpush(sock::IO,      key::AbstractString,   values::Any...;        if_exists=false)              = push(sock, key, values..., dir=:right, if_exists=if_exists);
rpushx(sock::IO,     key::AbstractString,   values::Any...)                                      = push(sock, key, values..., dir=:right, if_exists=true);

lrange(sock::IO,     key::AbstractString,   start::Integer,                stop::Integer)                  = send(sock,   "LRANGE",    key,       string(start),     string(stop))
lrem(sock::IO,       key::AbstractString,   count::Integer,                value::AbstractString)          = send(sock,   "LREM",      key,       string(count),     value)
lset(sock::IO,       key::AbstractString,   index::Integer,                value::AbstractString)          = send(sock,   "LSET",      key,       string(index),     value)
ltrim(sock::IO,      key::AbstractString,   start::Integer,                stop::Integer)                  = send(sock,   "LTRIM",     key,       string(start),     string(stop))
rpop(sock::IO,       key::AbstractString)                                                                  = send(sock,   "RPOP",      key)
rpoplpush(sock::IO,  skey::AbstractString,  dkey::AbstractString)                                          = send(sock,   "RPOPLPUSH", skey,      dkey)

# client only commands (w/ some help from the core method definitions)
lpop(sock::IO,       key::AbstractString,   count::Integer)                                        = [lpop(sock,  key) for i = 1:count]
lrange(sock::IO,     key::AbstractString)                                                          = lrange(sock, key, 0, -1)
rpop(sock::IO,       key::AbstractString,   count::Integer)                                        = [rpop(sock,  key) for i = 1:count]

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
#function dbsize(sock::IO, index::Integer) 
#	cdb = current(sock)
#	select!(sock, index)
#	r = send(sock, "DBSIZE")
#	select!(sock, cdb)
#	r
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
		throw(InvalidSectionException(section))
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
