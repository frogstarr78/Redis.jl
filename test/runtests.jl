using Redis
using Redis.decode_response
using Base.Test

not_in(x, a) = !(x in a)

# write your own tests here
@test 1 == 1
@test redis_type("") == "\$0\r\n\r\n"
@test redis_type("abc") == "\$3\r\nabc\r\n"
@test redis_type(None) == "\$-1\r\n"
#print("                                                         ")
#show(redis_type(["cmd", "key", "value"]))
#println("")
@test redis_type(["cmd", "key", "value"]) == "*3\r\n\$3\r\ncmd\r\n\$3\r\nkey\r\n\$5\r\nvalue\r\n"

#test simple string decodig
io = IOBuffer("+OK\r\n")
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError decode_response(io, "NOCOMMAND")
@test readall(io) == ""

io = IOBuffer("+OK\r\n")
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError decode_response(io, "nocommand")
@test readall(io) == ""

io = IOBuffer("+OK\r\n")
@test decode_response(io, "set") == "OK"
@test readall(io) == ""

io = IOBuffer("+OK\r\n")
@test decode_response(io, "SET") == "OK"
@test readall(io) == ""

#test boolean decoding
io = IOBuffer(":1\r\n")
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError decode_response(io, "nocommand")
@test readall(io) == ""

io = IOBuffer(":1\r\n")
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError decode_response(io, "NOCOMMAND")
@test readall(io) == ""

io = IOBuffer(":1\r\n")
@test decode_response(io, "exists") == true
@test readall(io) == ""

io = IOBuffer(":0\r\n")
@test decode_response(io, "exists") == false
@test readall(io) == ""

io = IOBuffer(":1\r\n")
@test decode_response(io, "EXISTS") == true
@test readall(io) == ""

io = IOBuffer(":0\r\n")
@test decode_response(io, "EXISTS") == false
@test readall(io) == ""

io = IOBuffer(":6\r\n")
@test decode_response(io, "exists") == true
@test readall(io) == ""

io = IOBuffer(":6\r\n")
@test decode_response(io, "EXISTS") == true
@test readall(io) == ""

#test number decoding
io = IOBuffer(":6\r\n")
@test decode_response(io, "strlen") == 6
@test readall(io) == ""

io = IOBuffer(":0\r\n")
@test decode_response(io, "strlen") == 0
@test readall(io) == ""

io = IOBuffer(":6\r\n")
@test decode_response(io, "STRLEN") == 6
@test readall(io) == ""

io = IOBuffer(":0\r\n")
@test decode_response(io, "STRLEN") == 0
@test readall(io) == ""

#test error decoding
io = IOBuffer("-Something went wrong\r\n")
@test_throws Exception decode_response(io, "strlen")
@test readall(io) == ""

#test complex string decoding
io = IOBuffer(string('$', "5\r\nhello\r\n"))
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError decode_response(io)
@test readall(io) == ""

io = IOBuffer(string('$', "5\r\nhello\r\n"))
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError decode_response(io)
@test readall(io) == ""


io = IOBuffer(string('$', "5\r\nhello\r\n"))
@test decode_response(io, "get") == "hello"
@test readall(io) == ""

io = IOBuffer(string('$', "3\r\nhello\r\n"))
@test decode_response(io, "get") == "hel"
@test readall(io) == ""
#veriftying that we're accurately parsing based on the prefix length value


#send(sock::IOBuffer, cmd, args...) = ( write(sock, redis_type([cmd, args])); decode_response(sock, cmd[1]) )
