using Redis
using Base.Test

not_in(x, a) = !(x in a)

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

io = IOBuffer(string('$', "-1\r\n"))
@test Redis.decode_response(io, "get") == ""
@test readall(io) == ""
