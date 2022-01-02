using Redis
using Base.Test

#not_in(x, a) = !(x in a)
not_in(x, a) = !any((y) -> x in y, values(a))

#test simple string decodig
klient = IOBuffer("+OK\r\n")
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "NOCOMMAND")
@test eof(klient) == true

klient = IOBuffer("+OK\r\n")
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "nocommand")
@test eof(klient) == true

klient = IOBuffer("+OK\r\n")
@test Redis.decode_response(klient, "set") == "OK"
@test eof(klient) == true

klient = IOBuffer("+OK\r\n")
@test Redis.decode_response(klient, "SET") == "OK"
@test eof(klient) == true

#test boolean decoding
klient = IOBuffer(":1\r\n")
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "nocommand")
@test eof(klient) == true

klient = IOBuffer(":1\r\n")
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "NOCOMMAND")
@test eof(klient) == true

klient = IOBuffer(":1\r\n")
@test Redis.decode_response(klient, "exists") == 1
@test eof(klient) == true

klient = IOBuffer(":0\r\n")
@test Redis.decode_response(klient, "exists") == 0
@test eof(klient) == true

klient = IOBuffer(":1\r\n")
@test Redis.decode_response(klient, "EXISTS") == 1
@test eof(klient) == true

klient = IOBuffer(":0\r\n")
@test Redis.decode_response(klient, "EXISTS") == 0
@test eof(klient) == true

klient = IOBuffer(":6\r\n")
@test Redis.decode_response(klient, "exists") == 6
@test eof(klient) == true

klient = IOBuffer(":6\r\n")
@test Redis.decode_response(klient, "EXISTS") == 6
@test eof(klient) == true

#test number decoding
klient = IOBuffer(":6\r\n")
@test Redis.decode_response(klient, "strlen") == 6
@test eof(klient) == true

klient = IOBuffer(":0\r\n")
@test Redis.decode_response(klient, "strlen") == 0
@test eof(klient) == true

klient = IOBuffer(":6\r\n")
@test Redis.decode_response(klient, "STRLEN") == 6
@test eof(klient) == true

klient = IOBuffer(":0\r\n")
@test Redis.decode_response(klient, "STRLEN") == 0
@test eof(klient) == true

#test error decoding
klient = IOBuffer("-Something went wrong\r\n")
@test_throws Exception Redis.decode_response(klient, "strlen")
@test eof(klient) == true

#test complex string decoding
s = string('$', "5\r\nhello\r\n")
klient = IOBuffer(s)
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "nocommand")
@test read(klient) == Vector{UInt8}("hello\r\n")

klient = IOBuffer(s)
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "NOCOMMAND")
@test read(klient) == Vector{UInt8}("hello\r\n")

klient = IOBuffer(s)
@test Redis.decode_response(klient, "get") == "hello"
@test eof(klient) == true

#veriftying that we're accurately parsing based on the prefix length value
klient = IOBuffer(string('$', "3\r\nhello\r\n"))
@test Redis.decode_response(klient, "get") == "hel"
@test eof(klient) == true

klient = IOBuffer(string('$', "-1\r\n"))
@test Redis.decode_response(klient, "get") == nothing
@test eof(klient) == true
