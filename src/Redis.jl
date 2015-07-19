module Redis

#import Base.TcpSocket
export redis_type, client

CRLF = "\r\n"
CRLFc = ['\r', '\n']
EXPECTATIONS = {
	Bool => Set(["hexists", "exists", "sismember", "hset", "sadd"]),
	Int => Set(["hklen", "scard", "llen", "strlen"]),
	Array => Set(["hkeys", "keys"]),
	Dict => Set(["hgetall"]),
	Set => Set(["smembers"]),
	String => Set(["set", "get"])
}

redis_type(arg::String; simple::Bool=false) = (simple ? string('+', arg, CRLF ) : string('$', join([length(arg), arg], CRLF), CRLF))
redis_type(arg::Number)                     = string(':', string(arg), CRLF)
redis_type(arg::UnionType)                  = string('$', string(-1), CRLF) # aka None
redis_type(arg::Array)                      = string('*', string(length(arg)), CRLF, mapreduce(redis_type, *, arg))
redis_type(arg::Tuple)                      = string(collect(arg))

function decode_response(sock::IO, cmd_called)
	resp = readuntil(sock, CRLF)
	resp = strip(resp, CRLFc)
#	println("running command $cmd_called resp $(resp)\nBools $(EXPECTATIONS[Bool])\nInts $(EXPECTATIONS[Int])")
	if resp[1] == '+'
		if lowercase(cmd_called) in EXPECTATIONS[String]
			return resp[2:end]
		else
			throw(KeyError(EXPECTATIONS))
		end
	elseif resp[1] == ':'
		if lowercase(cmd_called) in EXPECTATIONS[Int]
			return parseint(resp[2:end])
		elseif lowercase(cmd_called) in EXPECTATIONS[Bool]
			return parseint(resp[2:end]) > 0
		else
			throw(KeyError(EXPECTATIONS))
		end
	elseif resp[1] == '-'
		error(resp[2:end])
	elseif resp[1] == '$'
		if lowercase(cmd_called) in EXPECTATIONS[String]
			len = parseint(resp[2:end])
			r = join(map(char,readbytes(sock, len)))
			readuntil(sock, CRLF)
			return r
		else
			throw(KeyError(EXPECTATIONS))
		end
	elseif resp[1] == '*'
		if lowercase(cmd_called) in EXPECTATIONS[Dict]
			r = [decode_response(sock, cmd_called) for i = 1:parseint(resp[2:end])]
			ret = Dict{String,Any}()
			while !isempty(r)
				merge!(ret, [shift!(r) => shift!(r)])
			end
			return ret
		elseif lowercase(cmd_called) in EXPECTATIONS[Set]
			return Set{String}([decode_response(sock, cmd_called) for i = 1:parseint(resp[2:end])])
		else
			return [decode_response(sock, cmd_called) for i = 1:parseint(resp[2:end])]
		end
	else
		println("resp '$resp' == $(char(resp) == '+')")
	end
end

client(socket_path::String; password=None, db=0)            = connect(socket_path)
client(;host=ip"127.0.0.1", port=6379, password=None, db=0) = connect(host, port)

send(sock::IO, redis_cmd, args...)        = ( write(sock, redis_type([redis_cmd, args])); decode_response(sock, redis_cmd[1]) )

function set(sock::IO, key::String, value::Any; sec_expire::Int=-1, ms_expire::Float64=-1.0, not_exists::Bool=false, if_exists::Bool=false)
	cmd_msg = String["SET", key, value]
	sec_expire > -1 && push!(cmd_msg, "EX", sec_expire)	
	ms_expire > -1.0 && push!(cmd_msg, "PX", ms_expire)
	not_exists && if_exists && error("not_exists and if_exists cannot be set simultaneously")
	not_exists && push!(cmd_msg, "NX")
	if_exists && push!(cmd_msg, "XX")

	send(sock, cmd_msg...)
end
setnx(sock::IO,        key::String, value::Any) = set(sock, key, value, not_exists=true)
setex(sock::IO,        key::String, seconds::Int, value::Any) = set(sock, key, value, sec_expire=seconds)
psetex(sock::IO,       key::String, milliseconds::Int, value::Any) = set(sock, key, value, ms_expire=milliseconds)

del(sock::IO,          key::String...)                                                          = send(sock, "DEL",         key...)
set(sock::IO,          key::String,         val::String)                                        = send(sock, "GET",         key,         val)
set(sock::IO,          key::String,         val::Int64)                                         = send(sock, "GET",         key,         string(val))
set(sock::IO,          key::String,         val::Char)                                          = send(sock, "GET",         key,         string(val))
mset(sock::IO,         key_val::Array{ASCIIString,2})                                           = send(sock, "MSET",        key_val)
#mset(sock::IO,         key_val::Array{Int64,2})                                                 = send(sock, "MSET",        key_val)    
#mset(sock::IO,         key_val::Array{Char,2})                                                  = send(sock, "MSET",        key_val)    
msetnx(sock::IO,       key_val::Array{ASCIIString,2})                                           = send(sock, "MSETNX",      key_val)
#msetnx(sock::IO,       key_val::Array{Int64,2})                                                 = send(sock, "MSETNX",      key_val)    
#msetnx(sock::IO,       key_val::Array{Char,2})                                                  = send(sock, "MSETNX",      key_val)    
get(sock::IO,          key::String)                                                             = send(sock, "GET",         key)
mget(sock::IO,         key::Array{Any})                                                         = send(sock, "MGET",        keys...)
append(sock::IO,       key::String,         val::String)                                        = send(sock, "APPEND",      key,         value)
incr(sock::IO,         key::String)                                                             = send(sock, "INCR",        key)
incrby(sock::IO,       key::String,         by::Int64)                                          = send(sock, "INCRBY",      key,         by)
incrbyfloat(sock::IO,  key::String,         by::Float64)                                        = send(sock, "INCRBYFLOAT", key,         by)
decr(sock::IO,         key::String)                                                             = send(sock, "DECR",        key)
decrby(sock::IO,       key::String,         by::Int64)                                          = send(sock, "DECRBY",      key,         by)
decrbyfloat(sock::IO,  key::String,         by::Float64)                                        = send(sock, "DECRBY",      key,         by)
getrange(sock::IO,     key::String,         start::Int64,        nd::Int64)                     = send(sock, "GETRANGE",    key,         start,       nd)
bitcount(sock::IO,     key::String,         start::Int64,        nd::Int64)                     = send(sock, "BITCOUNT",    key,         start,       nd)
bitop(sock::IO,        op::String,          destkey::String,     key::String...)                = send(sock, "BITOP",       op,          destkey,     start,  nd)
bitpos(sock::IO,       key::String,         bit::String,         start::Int64,   nd::Int64)     = send(sock, "BITPOS",      key,         bit,         start,  nd)
getset(sock::IO,       key::String,         val::String)                                        = send(sock, "GETSET",      key,         val)
strlen(sock::IO,       key::String)                                                             = send(sock, "STRLEN",      key)
exists(sock::IO,       key::String)                                                             = send(sock, "EXISTS",      key)

hexists(sock::IO,      key::String,         hkey::Any)                                          = send(sock, "HEXISTS",     key,         hkey)

sadd(sock::IO,         key::String,         smems::Any...)                                      = send(sock, "SADD",        key,         smems...)
scard(sock::IO,        key::String)                                                             = send(sock, "SCARD",       key)
sdiff(sock::IO,        key::String,         keys::String...)                                    = send(sock, "SDIFF",       key,         keys...)
sdiffstore(sock::IO,   destination::String, key::String,         keys::String...)               = send(sock, "SDIFFSTORE",  destination, key,         keys...)
sinter(sock::IO,       key::String,         keys::String...)                                    = send(sock, "SINTER",      key,         keys...)
sinterstore(sock::IO,  destination::String, key::String,         keys::String...)               = send(sock, "SINTERSTORE", destination, key,         keys...)
sismember(sock::IO,    key::String,         smem::Any)                                          = send(sock, "SISMEMBER",   key,         smem)
smembers(sock::IO,     key::String)                                                             = send(sock, "SMEMBERS",    key)
smove(sock::IO,        source::String,      destination::String, member::Any)                   = send(sock, "SMOVE",       source,      destination, member)
spop(sock::IO,         key::String,         count::Int64=1)                                     = send(sock, "SPOP",        key,         count)
srandmember(sock::IO,  key::String,         count::Int64=1)                                     = send(sock, "SRANDMEMBER", key,         count)
srem(sock::IO,         key::String,         members::String...)                                 = send(sock, "SREM",        key,         members...)
sunion(sock::IO,       key::String,         keys::Any...)                                       = send(sock, "SUNION",      key,         members...)
sunionstore(sock::IO,  destination::String, key::String,         keys::String...)               = send(sock, "SUNIONSTORE", destination, key,         keys...)

#sscan(sock::IO,        key::String,         cursor::Int64;       matching::Regex, count::Int64) = send(sock, "SSCAN",       key,         cursor,      matching, count)

end # module
