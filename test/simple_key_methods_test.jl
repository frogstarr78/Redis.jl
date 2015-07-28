using Redis
using Base.Test

function test_append_methods(io)
	@test Redis.get(io, "KEYAPPEND") == ""
	@test Redis.append(io, "KEYAPPEND", "VAL") == 3
	@test Redis.get(io, "KEYAPPEND") == "VAL"
	close(io)
end

function test_incr_methods(io)
	@test Redis.incr(io, "KEY") == 1
	@test Redis.incrby(io, "KEY", 4) == 5
	r = Redis.incrby(io, "KEY", 4.1)
	@test string(r) == "9.1"
	r = Redis.incrbyfloat(io, "KEY", 3.2)
	@test_approx_eq r 12.3
	@test string(r) == "12.3"
	close(io)
end

function test_decr_methods(io)
	r = Redis.set(io, "KEY", 12)
	@test r == "OK"
	r = Redis.decr(io, "KEY") 
	@test r == 11
	r = Redis.decrby(io, "KEY", 4)
	@test r == 7
	warn("Currently, server doesn't implement decrbyfloat.")
#	r = Redis.decrby(io, "KEY", 4.1)
#	@test string(r) == "3.2"
#	r = Redis.decrbyfloat(io, "KEY", 3.2)
#	@printf "r '%s'" r
#	@test string(r) == "0"
	close(io)
end

function test_mset_methods(io)
	@test Redis.mset(io, [["MSETKEY" "MSETKEY2"], ["VAL" "VAL2"]]::Array{ASCIIString,2}) == "OK"
	@test sort(Redis.mget(io, ["MSETKEY", "MSETKEY2"])) == sort(["VAL", "VAL2"]) == Redis.mget(io, ["MSETKEY", "MSETKEY2"], sorted=true)

	@test Redis.set(io, ["SETMSETKEY" "SETMSETVAL"]) == "OK"
	@test Redis.get(io, "SETMSETKEY") == "SETMSETVAL"

	@test sort(Redis.mget(io, "SETMSETKEY", "MSETKEY")) == sort(["VAL", "SETMSETVAL"]) == Redis.mget(io, "SETMSETKEY", "MSETKEY", sorted=true)

	@test Redis.mset(io, ['C' 'V']) == "OK"
	@test Redis.get(io, 'C') == "V"

	@test Redis.set(io, ['D' 'W']) == "OK"
	@test Redis.get(io, 'D') == "W"

	@test Redis.set(io, ['A', 'a', 536, 234, "STR", "str"]) == "OK"
	@test sort(Redis.get(io, ['A', 'C', 536, 'D', "STR"])) == sort(["V", "W", "str", "a", "234"])

	@test Redis.set(io, 'B', 'b', 535, 233, "STS") == "OK"
	@test sort(Redis.get(io, 'B', 535, "STS")) == sort(["b", "233", ""]) == Redis.get(io, 'B', 535, "STS", sorted=true)

	@test Redis.mset(io, [123 456]) == "OK"
	@test Redis.get(io, 123) == "456"

	@test sort(Redis.mget(io, 123, 'D', "SETMSETKEY")) == sort(["456", "W", "SETMSETVAL"]) == Redis.mget(io, 123, 'D', "SETMSETKEY", sorted=true)

	@test Redis.msetnx(io, [["MSETNXKEY" "MSETNXKEY2"], ["MSETNXVAL" "MSETNXVAL2"]]) == true
	@test Redis.msetnx(io, ["MSETNXKEY", "MSETNXVAL"]) == false
	@test Redis.msetnx(io, [['F' 'J'], ['V' 'W']]) == true
	@test Redis.msetnx(io, [321, 345]) == true
	@test sort(Redis.mget(io, 'F', "MSETNXKEY", 321, 'J')) == sort(["345", "V", "W", "MSETNXVAL"]) == Redis.mget(io, 'F', "MSETNXKEY", 321, 'J', sorted=true)

	@test Redis.msetnx(io, "abc", "abc", "xyz", "xyz") == true
	@test sort(Redis.mget(io, "abc", "xyz")) == sort(["abc", "xyz"]) == Redis.mget(io, "abc", "xyz", sorted=true)
end

function test_standard_key_methods(io)
	@test Redis.set(io, "HELLO", "The quick brown fox jumped over the lazy dog.") == "OK"
	@test Redis.set(io, "INTVAL", 123) == "OK"
	@test Redis.get(io, "INTVAL") == "123"
	@test Redis.set(io, "CHARVAL", 'c') == "OK"
	@test Redis.get(io, "CHARVAL") == "c"
	@test Redis.get(io, "HELLO") == "The quick brown fox jumped over the lazy dog."
	@test Redis.getrange(io, "HELLO", 4, 8) == "quick"
	@test Redis.strlen(io, "HELLO") == 45
	@test Redis.bitcount(io, "HELLO") == 167
	@test Redis.bitcount(io, "HELLO", 0, -1) == 167
	@test Redis.bitcount(io, "HELLO", 1, 3) == 8
	@test Redis.bitpos(io, "HELLO", 1) == 1
	@test Redis.bitpos(io, "HELLO", 0) == 0
	@test Redis.bitpos(io, "HELLO", 1, 0, -1) == 1
	@test Redis.bitpos(io, "HELLO", 0, 0, -1) == 0
	@test Redis.bitpos(io, "HELLO", 1, 5, 10) == 41
	@test Redis.bitpos(io, "HELLO", 0, 5, 10) == 40
	warn("Don't have the documentation for bitop to test it atm.")
	@test Redis.getbit(io, "HELLO", 11) == 0
	@test Redis.setbit(io, "HELLO", 11, 1) == 0
	@test Redis.getbit(io, "HELLO", 11) == 1
	@test Redis.getset(io, "HELLO", "Nothing new") == "Txe quick brown fox jumped over the lazy dog."
	@test Redis.get(io, "HELLO") == "Nothing new"
	@test Redis.ttl(io, "HELLO") == -1
	@test Redis.pttl(io, "HELLO") == -1
	close(io)
end

run(`redis-server ${pwd()}/etc/redis.conf`)
try
	test_append_methods(client(port=9999))
	test_incr_methods(client(port=9999))
	test_decr_methods(client(port=9999))
	test_standard_key_methods(client(port=9999))
	test_mset_methods(client(port=9999))
catch e
	error(e)
finally
	run(`redis-cli -p 9999 SHUTDOWN`)
end
