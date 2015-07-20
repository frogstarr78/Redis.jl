using Redis: client, send, redis_type
using Base.Test

not_in(x, a) = !(x in a)

# write your own tests here
@test 1 == 1
@test Redis.redis_type("") == string('$', "0\r\n\r\n")
@test Redis.redis_type("abc") == string('$', "3\r\nabc\r\n")
@test Redis.redis_type(None) == string('$', "-1\r\n")
#print("                                                         ")
#show(Redis.redis_type(["cmd", "key", "value"]))
#println("")
@test Redis.redis_type(["cmd", "key", "value"]) == string("*3\r\n", '$', "3\r\ncmd\r\n", '$', "3\r\nkey\r\n", '$', "5\r\nvalue\r\n")

#test simple string decodig
io = IOBuffer("+OK\r\n")
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(io, "NOCOMMAND")
@test readall(io) == ""

io = IOBuffer("+OK\r\n")
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(io, "nocommand")
@test readall(io) == ""

io = IOBuffer("+OK\r\n")
@test Redis.decode_response(io, "set") == "OK"
@test readall(io) == ""

io = IOBuffer("+OK\r\n")
@test Redis.decode_response(io, "SET") == "OK"
@test readall(io) == ""

#test boolean decoding
io = IOBuffer(":1\r\n")
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(io, "nocommand")
@test readall(io) == ""

io = IOBuffer(":1\r\n")
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(io, "NOCOMMAND")
@test readall(io) == ""

io = IOBuffer(":1\r\n")
@test Redis.decode_response(io, "exists") == true
@test readall(io) == ""

io = IOBuffer(":0\r\n")
@test Redis.decode_response(io, "exists") == false
@test readall(io) == ""

io = IOBuffer(":1\r\n")
@test Redis.decode_response(io, "EXISTS") == true
@test readall(io) == ""

io = IOBuffer(":0\r\n")
@test Redis.decode_response(io, "EXISTS") == false
@test readall(io) == ""

io = IOBuffer(":6\r\n")
@test Redis.decode_response(io, "exists") == true
@test readall(io) == ""

io = IOBuffer(":6\r\n")
@test Redis.decode_response(io, "EXISTS") == true
@test readall(io) == ""

#test number decoding
io = IOBuffer(":6\r\n")
@test Redis.decode_response(io, "strlen") == 6
@test readall(io) == ""

io = IOBuffer(":0\r\n")
@test Redis.decode_response(io, "strlen") == 0
@test readall(io) == ""

io = IOBuffer(":6\r\n")
@test Redis.decode_response(io, "STRLEN") == 6
@test readall(io) == ""

io = IOBuffer(":0\r\n")
@test Redis.decode_response(io, "STRLEN") == 0
@test readall(io) == ""

#test error decoding
io = IOBuffer("-Something went wrong\r\n")
@test_throws Exception Redis.decode_response(io, "strlen")
@test readall(io) == ""

#test complex string decoding
s = string('$', "5\r\nhello\r\n")
io = IOBuffer(s)
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(io, "nocommand")
@test readall(io) == "hello\r\n"

io = IOBuffer(s)
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(io, "NOCOMMAND")
@test readall(io) == "hello\r\n"


io = IOBuffer(s)
@test Redis.decode_response(io, "get") == "hello"
@test readall(io) == ""

#veriftying that we're accurately parsing based on the prefix length value
io = IOBuffer(string('$', "3\r\nhello\r\n"))
@test Redis.decode_response(io, "get") == "hel"
@test readall(io) == ""

#Redis.send(sock::IOBuffer, redis_cmd, args...)        = ( write(sock, Redis.redis_type([redis_cmd, args...])); seekstart(sock); Redis.decode_response(sock, redis_cmd) )

io = IOBuffer(true, true)
s = string("*3\r\n", '$', "3\r\nSET\r\n", '$', "3\r\nKEY\r\n", '$', "5\r\nVALUE\r\n")
r =  Redis.send(io, "SET", "KEY", "VALUE")
@test r == ["SET", "KEY", "VALUE"]
