module Redis

import Base.TcpSocket
export redis_type, client

CRLF = "\r\n"
CRLFc = ['\r', '\n']
EXPECTATIONS = {
	Bool => Set(["hexists", "exists", "sismember", "hset", "sadd"]),
	Int => Set(["hklen", "scard", "llen", "strlen"]),
	Array => Set(["hkeys"]),
	Dict => Set(["hgetall"]),
	Set => Set(["smembers"])
}

redis_type(arg::String; simple::Bool=false) = (simple ? string('+', arg, CRLF ) : string('$', join([length(arg), arg], CRLF), CRLF))
redis_type(arg::Number)                     = string(':', string(arg), CRLF)
redis_type(arg::UnionType)                  = string('$', string(-1), CRLF) # aka None
redis_type(arg::Array)                      = string('*', string(length(arg)), CRLF, mapreduce(redis_type, *, arg))
redis_type(arg::Tuple)                      = string('*', string(length(arg)), CRLF, mapreduce(redis_type, *, arg))

function decode_response(sock::TcpSocket, cmd_called)
	resp = readuntil(sock, CRLF)
	resp = strip(resp, CRLFc)
#	println("running command $cmd_called resp $(resp)\nBools $(EXPECTATIONS[Bool])\nInts $(EXPECTATIONS[Int])")
	if resp[1] == '+'
		return resp[2:end]
	elseif resp[1] == ':'
		if lowercase(cmd_called) in EXPECTATIONS[Int]
			return parseint(resp[2:end])
		elseif lowercase(cmd_called) in EXPECTATIONS[Bool]
			return parseint(resp[2:end]) == 1
		end
	elseif resp[1] == '-'
		error(resp[2:end])
	elseif resp[1] == '$'
		len = parseint(resp[2:end])
		r = join(map(char,readbytes(sock, len)))
		readuntil(sock, CRLF)
		return r
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

send(sock::TcpSocket, cmd...)        = ( write(sock, redis_type(cmd)); decode_response(sock, cmd[1]) )
exists(sock::TcpSocket, key::String) = send(sock, ["EXISTS", key]...)

function set(sock::TcpSocket, key::String, value::Any; sec_expire::Int=-1, ms_expire::Float64=-1.0, not_exists::Bool=false, if_exists::Bool=false)
	cmd_msg = String["SET", key, value]
	sec_expire > -1 && push!(cmd_msg, "EX", sec_expire)	
	ms_expire > -1.0 && push!(cmd_msg, "PX", ms_expire)
	not_exists && if_exists && error("not_exists and if_exists cannot be set simultaneously")
	not_exists && push!(cmd_msg, "NX")
	if_exists && push!(cmd_msg, "XX")

	send(sock, cmd_msg...)
end
setnx(sock::TcpSocket, key::String, value::Any) = set(sock, key, value, not_exists=true)
setex(sock::TcpSocket, key::String, seconds::Int, value::Any) = set(sock, key, value, sec_expire=seconds)
psetex(sock::TcpSocket, key::String, milliseconds::Int, value::Any) = set(sock, key, value, ms_expire=milliseconds)

get(sock::TcpSocket, key::String)                  = send(sock, ["GET", key]...)
exists(sock::TcpSocket, key::String)               = send(sock, ["EXISTS", key]...)
hexists(sock::TcpSocket, key::String, hkey::Any)   = send(sock, ["HEXISTS", key, hkey]...)
sismember(sock::TcpSocket, key::String, smem::Any) = send(sock, ["SISMEMBER", key, smem]...)

end # module
