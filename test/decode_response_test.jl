using Redis
using Base.Test

#not_in(x, a) = !(x in a)
not_in(x, a) = !any((y) -> x in y, values(a))

#test simple string decodig
klient = IOBuffer("+OK\r\n")
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "NOCOMMAND")
@test readall(klient) == ""

klient = IOBuffer("+OK\r\n")
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "nocommand")
@test readall(klient) == ""

klient = IOBuffer("+OK\r\n")
@test Redis.decode_response(klient, "set") == "OK"
@test readall(klient) == ""

klient = IOBuffer("+OK\r\n")
@test Redis.decode_response(klient, "SET") == "OK"
@test readall(klient) == ""

#test boolean decoding
klient = IOBuffer(":1\r\n")
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "nocommand")
@test readall(klient) == ""

klient = IOBuffer(":1\r\n")
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "NOCOMMAND")
@test readall(klient) == ""

klient = IOBuffer(":1\r\n")
@test Redis.decode_response(klient, "exists") == 1
@test readall(klient) == ""

klient = IOBuffer(":0\r\n")
@test Redis.decode_response(klient, "exists") == 0
@test readall(klient) == ""

klient = IOBuffer(":1\r\n")
@test Redis.decode_response(klient, "EXISTS") == 1
@test readall(klient) == ""

klient = IOBuffer(":0\r\n")
@test Redis.decode_response(klient, "EXISTS") == 0
@test readall(klient) == ""

klient = IOBuffer(":6\r\n")
@test Redis.decode_response(klient, "exists") == 6
@test readall(klient) == ""

klient = IOBuffer(":6\r\n")
@test Redis.decode_response(klient, "EXISTS") == 6
@test readall(klient) == ""

#test number decoding
klient = IOBuffer(":6\r\n")
@test Redis.decode_response(klient, "strlen") == 6
@test readall(klient) == ""

klient = IOBuffer(":0\r\n")
@test Redis.decode_response(klient, "strlen") == 0
@test readall(klient) == ""

klient = IOBuffer(":6\r\n")
@test Redis.decode_response(klient, "STRLEN") == 6
@test readall(klient) == ""

klient = IOBuffer(":0\r\n")
@test Redis.decode_response(klient, "STRLEN") == 0
@test readall(klient) == ""

#test error decoding
klient = IOBuffer("-Something went wrong\r\n")
@test_throws Exception Redis.decode_response(klient, "strlen")
@test readall(klient) == ""

#test complex string decoding
s = string('$', "5\r\nhello\r\n")
klient = IOBuffer(s)
@test not_in("nocommand", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "nocommand")
@test readall(klient) == "hello\r\n"

klient = IOBuffer(s)
@test not_in("NOCOMMAND", Redis.EXPECTATIONS)
@test_throws KeyError Redis.decode_response(klient, "NOCOMMAND")
@test readall(klient) == "hello\r\n"

klient = IOBuffer(s)
@test Redis.decode_response(klient, "get") == "hello"
@test readall(klient) == ""

#veriftying that we're accurately parsing based on the prefix length value
klient = IOBuffer(string('$', "3\r\nhello\r\n"))
@test Redis.decode_response(klient, "get") == "hel"
@test readall(klient) == ""

klient = IOBuffer(string('$', "-1\r\n"))
@test Redis.decode_response(klient, "get") == ""
@test readall(klient) == ""
