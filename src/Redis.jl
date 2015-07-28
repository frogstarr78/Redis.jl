module Redis

import Base.TcpSocket
export client

CRLFc = ['\r', '\n']
CRLF = join(CRLFc)
EXPECTATIONS = {
	Array => Set(["hkeys", "keys", "mget"]),
	Bool => Set(["hexists", "exists", "sismember", "hset", "sadd", "del", "msetnx"]),
	Dict => Set(["hgetall"]),
	Float64 => Set(["incrbyfloat"]),
	Int => Set([
		"append",
		"bitcount",
		"bitpos",
		"dbsize",
		"decr",
		"decrby",
		"getbit",
		"hklen",
		"incr",
		"incrby",
		"llen",
		"pttl",
		"scard",
		"setbit",
		"setrange",
		"strlen",
		"ttl"
	]),
	Set => Set(["smembers"]),
	String => Set([
		"auth",
		"bgrewriteaof",
		"bgsave",
		"del",
		"flushall",
		"flushdb",
		"get",
		"getrange",
		"getset",
		"info", 
		"mset",
		"ping",
		"save",
		"set",
		"type"
	]),
	TmStruct => Set(["lastsave", "time"])
}

redis_type(arg::String; simple::Bool=false) = (simple ? string('+', arg, CRLF ) : string('$', join([length(arg), arg], CRLF), CRLF))
redis_type(arg::UnionType)                  = string('$', -1, CRLF)
redis_type(arg::Number)                     = string(':', arg, CRLF)
function redis_type(arg::Array)
	len = length(arg)
	if len == 1
		return string('*', len, CRLF, redis_type(arg[1]))
	elseif len > 1
		return string('*', len, CRLF, mapreduce(redis_type, *, arg))
	end
end
#redis_type(arg::Tuple)                      = collect(arg)

function decode_response(sock::IO, cmd_called, sub_parse=false)
	resp = readuntil(sock, CRLF)
	resp = strip(resp, CRLFc)
#	println("running command '$cmd_called' resp '$resp'")
	if resp[1] == '+'
		if lowercase(cmd_called) in EXPECTATIONS[String] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
			return resp[2:end]
		else
			throw(KeyError(EXPECTATIONS))
		end
	elseif resp[1] == ':'
		if lowercase(cmd_called) in EXPECTATIONS[Int] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
			return parseint(resp[2:end])
		elseif lowercase(cmd_called) in EXPECTATIONS[TmStruct]
			return TmStruct(resp[2:end])
		elseif lowercase(cmd_called) in EXPECTATIONS[Bool] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
			return parseint(resp[2:end]) > 0
		else
			throw(KeyError(EXPECTATIONS))
		end
	elseif resp[1] == '-'
		error(resp[2:end])
	elseif resp[1] == '$'
		if lowercase(cmd_called) in EXPECTATIONS[String] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
			len = parseint(resp[2:end])
			if len == -1
				return ""
			else
				r = join(map(char,readbytes(sock, len)))
				readuntil(sock, CRLF)
				return r
			end
		elseif lowercase(cmd_called) in EXPECTATIONS[Float64] || ( sub_parse && lowercase(cmd_called) in EXPECTATIONS[Array] )
			len = parseint(resp[2:end])
			r = join(map(char,readbytes(sock, len)))
			readuntil(sock, CRLF)
			return parsefloat(r)
		elseif lowercase(cmd_called) in EXPECTATIONS[TmStruct] && sub_parse
			len = parseint(resp[2:end])
			r = join(map(char,readbytes(sock, len)))
			readuntil(sock, CRLF)
			return r
		else
			throw(KeyError(EXPECTATIONS))
		end
	elseif resp[1] == '*'
		len = parseint(resp[2:end])
		if lowercase(cmd_called) in EXPECTATIONS[Dict]
			r = [decode_response(sock, cmd_called, true) for i = 1:len]
			ret = Dict{String,Any}()
			while !isempty(r)
				merge!(ret, [shift!(r) => shift!(r)])
			end
			return ret
		elseif lowercase(cmd_called) in EXPECTATIONS[Set]
			return Set{String}([decode_response(sock, cmd_called, true) for i = 1:len])
		elseif lowercase(cmd_called) in EXPECTATIONS[Array]
			return [decode_response(sock, cmd_called, true) for i = 1:len]
		elseif lowercase(cmd_called) in EXPECTATIONS[TmStruct]
			secs, mills = [decode_response(sock, cmd_called, true) for i = 1:len]
			return TmStruct(parseint(secs))
		else
			return [decode_response(sock, cmd_called, true) for i = 1:len]
#			throw(KeyError(EXPECTATIONS))
		end
	else
		throw(KeyError(EXPECTATIONS))
	end
end

function client(;socket::String="", host=ip"127.0.0.1", port=6379, password=None, db=0)
	c = connect(host, port)
	password == None || auth(c, password)
	return c
end

send(sock::TcpSocket,          redis_cmd::String, args...) = ( write(sock, redis_type([redis_cmd, args...]));                     decode_response(sock, redis_cmd) )
send(sock::IOBuffer,           redis_cmd::String, args...) = ( write(sock, redis_type([redis_cmd, args...])); seekstart(sock);    decode_response(sock, redis_cmd) )
#client_send(sock::TcpSocket,  redis_cmd::String, args...) = ( write(sock, redis_type([string("CLIENT ",  redis_cmd), args...])); decode_response(sock, redis_cmd) )
#cluster_send(sock::TcpSocket, redis_cmd::String, args...) = ( write(sock, redis_type([string("CLUSTER ", redis_cmd), args...])); decode_response(sock, redis_cmd) )
#command_send(sock::TcpSocket, redis_cmd::String, args...) = ( write(sock, redis_type([string("COMMAND ", redis_cmd), args...])); decode_response(sock, redis_cmd) )
#config_send(sock::TcpSocket,  redis_cmd::String, args...) = ( write(sock, redis_type([string("CONFIG ",  redis_cmd), args...])); decode_response(sock, redis_cmd) )
#debug_send(sock::TcpSocket,   redis_cmd::String, args...) = ( write(sock, redis_type([string("DEBUG ",   redis_cmd), args...])); decode_response(sock, redis_cmd) )
#script_send(sock::TcpSocket,  redis_cmd::String, args...) = ( write(sock, redis_type([string("SCRIPT ",  redis_cmd), args...])); decode_response(sock, redis_cmd) )

#!untyped/introspective
auth(sock::IO,          password::String)                                                                  =  send(sock, "AUTH",         password)
bgrewriteaof(sock::IO)                                                                                     =  send(sock, "BGREWRITEAOF")
bgsave(sock::IO)                                                                                           =  save(sock,                 background=true)
dbsize(sock::IO)                                                                                           =  send(sock, "DBSIZE")
del(sock::IO,           keys::String...)                                                                   =  send(sock, "DEL",          keys...)
#dump
#echo
#eval
#evalsha
#expire
#expireat
exists(sock::IO,        key::String)                                                                       =  send(sock, "EXISTS",       key)
flushall(sock::IO)                                                                                         =  send(sock, "FLUSHALL")
flushdb(sock::IO)                                                                                          =  send(sock, "FLUSHDB")
info(sock::IO)                                                                                             =  send(sock, "INFO")
keys(sock::IO,          matching::String="*")                                                              =  send(sock, "KEYS",         matching)
lastsave(sock::IO)                                                                                         =  send(sock, "LASTSAVE")
#migrate
#monitor
#move
#object
#persist
#pexpire
#pexpireat
ping(sock::IO)                                                                                             =  send(sock, "PING")             
pttl(sock::IO,          key::String)                                                                       =  send(sock, "PTTL",         key)
#quit
#randomkey
#rename
#renamenx
#restore
#role
save(sock::IO;          background=false)                                                                  =  send(sock, background ? "BGSAVE" : "SAVE")
#scan
#select
#shutdown
#slaveof
#slowlog
#sort
#sync
time(sock::IO)                                                                                             =  send(sock, "TIME")
ttl(sock::IO,           key::String)                                                                       =  send(sock, "TTL",          key)
typeof(sock::IO,        key::String)                                                                       =  send(sock, "TYPE",         key)
#unwatch
#wait
#watch

#!transactional
#multi
#discard
#exec

#!std keys
append(sock::IO,        key::String,                  val::String)                                        = send(sock, "APPEND",            key,             val)

bitcount(sock::IO,      key::String,                  start::Int64=0,     nd::Int64=-1)                   = send(sock, "BITCOUNT",          key,             string(start), string(nd))
bitop(sock::IO,         op::String,                   destkey::String,    key::String...)                 = send(sock, "BITOP",             op,              destkey,       key...)
bitpos(sock::IO,        key::String,                  bit::Int,           start::Int64=0,  nd::Int64=-1)  = send(sock, "BITPOS",            key,             string(bit),   string(start),  string(nd))
getbit(sock::IO,        key::String,                  bit::Int)                                           = send(sock, "GETBIT",            key,             string(bit))
setbit(sock::IO,  key::String,      bit::Int,          value::Any) = send(sock, "SETBIT",          key,          string(bit),            string(value))

decr(sock::IO,          key::String)                                                                      = send(sock, "DECR",              key)
decrby(sock::IO,        key::String,                  by::Int64)                                          = send(sock, "DECRBY",            key,             string(by))
#decrby(sock::IO,       key::String,                  by::Float64)                                        = send(sock, "DECRBYFLOAT",       key,             string(by))
#decrbyfloat(sock::IO,  key::String,                  by::Float64)                                        = send(sock, "DECRBYFLOAT",       key,             string(by))
incr(sock::IO,          key::String)                                                                      = send(sock, "INCR",              key)
incrby(sock::IO,        key::String,                  by::Float64)                                        = send(sock, "INCRBYFLOAT",       key,             string(by))
incrby(sock::IO,        key::String,                  by::Int64)                                          = send(sock, "INCRBY",            key,             string(by))
incrbyfloat(sock::IO,   key::String,                  by::Real)                                           = send(sock, "INCRBYFLOAT",       key,             string(by))

getrange(sock::IO,      key::String,                  start::Int64,       nd::Int64)                      = send(sock, "GETRANGE",          key,             string(start), string(nd))
setrange(sock::IO,      key::String,                  start::Int64,       value::Any)                     = send(sock, "SETRANGE",          key,             string(start),        string(value))
getset(sock::IO,        key::String,                  val::String)                                        = send(sock, "GETSET",            key,             val)

get(sock::IO,           key::String)                                                                      = send(sock, "GET",               key)
get(sock::IO,           key::Char)                                                                        = get(sock,  string(key))
get(sock::IO,           key::Int64)                                                                       = get(sock,  string(key))


mget(sock::IO,          keys::Array{ASCIIString};     sorted=false)                                       = ( r =      send(sock, "MGET",   keys...);        sorted ? sort(r) : r )
mget(sock::IO,          keys::Any...;                 sorted=false)                                       = mget(sock, collect(map(string,  keys)),          sorted=sorted)
mget(sock::IO,          keys::Array;                  sorted=false)                                       = mget(sock, collect(map(string,  keys)),          sorted=sorted)
get(sock::IO,           keys::Any...;                 sorted=false)                                       = mget(sock, keys...,             sorted=sorted)
get(sock::IO,           keys::Array;                  sorted=false)                                       = mget(sock, keys...,             sorted=sorted)

mset(sock::IO,          key_val::Array{ASCIIString};  not_exists=false)                                   = send(sock, not_exists ?         "MSETNX" :       "MSET",    key_val...)
mset(sock::IO,          key_val::Array;               not_exists=false)                                   = mset(sock, collect(map(string,  key_val)),       not_exists=not_exists)
mset(sock::IO,          keys::Any...;                 not_exists=false)                                   = mset(sock, collect(map(string,  keys)),          not_exists=not_exists)
msetnx(sock::IO,        key_val::Array)                                                                   = mset(sock, collect(map(string,  key_val)),       not_exists=true)
msetnx(sock::IO,        keys::Any...)                                                                     = mset(sock, collect(map(string,  keys)),          not_exists=true)

function set(sock::IO, key::String, value::Any; sec_expire::Int=-1, ms_expire::Int=-1, not_exists::Bool=false, if_exists::Bool=false)
	cmd_msg = String["SET", key, value]
	sec_expire > -1 && push!(cmd_msg, "EX", string(sec_expire))
	ms_expire > -1 && push!(cmd_msg, "PX", string(ms_expire))
	not_exists && if_exists && error("not_exists and if_exists cannot be set simultaneously")
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

set(sock::IO,     key::String,      val::Char)                     = set(sock,  key,               string(val))
set(sock::IO,     key::String,      val::Int64)                    = set(sock,  key,               string(val))
set(sock::IO,     key_val::Any...)                                 = set(sock,  collect(key_val))
setex(sock::IO,   key::String,      seconds::Int,      value::Any) = set(sock,  key,               value,        sec_expire=seconds)
setnx(sock::IO,   key::String,      value::Any)                    = set(sock,  key,               value,        not_exists=true)
psetex(sock::IO,  key::String,      milliseconds::Int, value::Any) = set(sock,  key,               value,        ms_expire=milliseconds)
strlen(sock::IO,  key::String)                                     = send(sock, "STRLEN",          key)
#!end std keys
                                                                                                           
#!hashes
#hdel
hexists(sock::IO,       key::String,                 hkey::Any)                                            =  send(sock, "HEXISTS",      key,         hkey)
#hget
#hgetall
#hincrby
#hincrbyfloat
#hkeys
#hlen
#hmget
#hmset
#hscan
#hset
#hsetnx
#hstrlen
#hvals
#!end hashes
                                                                                                           
#!sets
sadd(sock::IO,          key::String,                 smems::Any...)                                        =  send(sock, "SADD",         key,         smems...)
scard(sock::IO,         key::String)                                                                       =  send(sock, "SCARD",        key)
sdiff(sock::IO,         key::String,                 keys::String...)                                      =  send(sock, "SDIFF",        key,         keys...)
sdiffstore(sock::IO,    destination::String,         key::String,          keys::String...)                =  send(sock, "SDIFFSTORE",   destination, key,                     keys...)
sinter(sock::IO,        key::String,                 keys::String...)                                      =  send(sock, "SINTER",       key,         keys...)
sinterstore(sock::IO,   destination::String,         key::String,          keys::String...)                =  send(sock, "SINTERSTORE",  destination, key,                     keys...)
sismember(sock::IO,     key::String,                 smem::Any)                                            =  send(sock, "SISMEMBER",    key,         smem)
smembers(sock::IO,      key::String)                                                                       =  send(sock, "SMEMBERS",     key)
smove(sock::IO,         source::String,              destination::String,  member::Any)                    =  send(sock, "SMOVE",        source,      destination,             member)
spop(sock::IO,          key::String,                 count::Int64=1)                                       =  send(sock, "SPOP",         key,         string(count))
srandmember(sock::IO,   key::String,                 count::Int64=1)                                       =  send(sock, "SRANDMEMBER",  key,         string(count))
srem(sock::IO,          key::String,                 members::String...)                                   =  send(sock, "SREM",         key,         members...)
#sscan(sock::IO,        key::String,                 cursor::Int64;        matching::Regex,  count::Int64) =  send(sock, "SSCAN",        key,         cursor,      matching,  count)
sunion(sock::IO,        key::String,                 keys::Any...)                                         =  send(sock, "SUNION",       key,         members...)
sunionstore(sock::IO,   destination::String,         key::String,          keys::String...)                =  send(sock, "SUNIONSTORE",  destination, key,                     keys...)
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
#zadd
#zcard
#zcount
#zincrby
#zinterstore
#zlexcount
#zrange
#zrangebylex
#zrangebyscore
#zrank
#zrem
#zremrangebylex
#zremrangebyrank
#zremrangebyscore
#zrevrange
#zrevrangebylex
#zrevrangebyscore
#zrevrank
#zscan
#zscore
#zunionstore
#!end sorted sets

#!lists
#blpop
#brpop
#brpoplpush
#lindex
#linsert
#llen
#lpop
#lpush
#lpushx
#lrange
#lrem
#lset
#ltrim
#rpop
#rpoplpush
#rpush
#rpushx
#!end lists

#debug object
#debug segfault

#script exists
#script flush
#script kill
#script load

#client getname
#client kill
#client list
#client pause
#client setname

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

#command count
#command getkeys
#command info
#command

#config get
#config resetstat
#config rewrite
#config set

end # module
