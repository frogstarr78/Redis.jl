using Redis
using Base.Test

function test_append_methods(client)
	@test Redis.get(client, "KEYAPPEND") == ""
	@test Redis.append(client, "KEYAPPEND", "VAL") == 3
	@test Redis.get(client, "KEYAPPEND") == "VAL"
	close(client)
end

function test_incr_methods(client)
	@test Redis.incr(client, "KEY") == 1
	@test Redis.incrby(client, "KEY", 4) == 5
	r = Redis.incrby(client, "KEY", 4.1)
	@test string(r) == "9.1"
	r = Redis.incrbyfloat(client, "KEY", 3.2)
	@test_approx_eq r 12.3
	@test string(r) == "12.3"
	close(client)
end

function test_decr_methods(client)
	r = Redis.set(client, "KEY", 12)
	@test r == "OK"
	r = Redis.decr(client, "KEY") 
	@test r == 11
	r = Redis.decrby(client, "KEY", 4)
	@test r == 7
	warn("Currently, server doesn't implement decrbyfloat.")
#	r = Redis.decrby(client, "KEY", 4.1)
#	@test string(r) == "3.2"
#	r = Redis.decrbyfloat(client, "KEY", 3.2)
#	@printf "r '%s'" r
#	@test string(r) == "0"
	close(client)
end

function test_mset_methods(client)
	@test Redis.mset(client, [["MSETKEY" "MSETKEY2"], ["VAL" "VAL2"]]::Array{ASCIIString,2}) == "OK"
	@test sort(Redis.mget(client, ["MSETKEY", "MSETKEY2"])) == sort(["VAL", "VAL2"]) == Redis.mget(client, ["MSETKEY", "MSETKEY2"], sorted=true)

	@test Redis.set(client, ["SETMSETKEY" "SETMSETVAL"]) == "OK"
	@test Redis.get(client, "SETMSETKEY") == "SETMSETVAL"

	@test sort(Redis.mget(client, "SETMSETKEY", "MSETKEY")) == sort(["VAL", "SETMSETVAL"]) == Redis.mget(client, "SETMSETKEY", "MSETKEY", sorted=true)

	@test Redis.mset(client, ['C' 'V']) == "OK"
	@test Redis.get(client, 'C') == "V"

	@test Redis.set(client, ['D' 'W']) == "OK"
	@test Redis.get(client, 'D') == "W"

	@test Redis.set(client, ['A', 'a', 536, 234, "STR", "str"]) == "OK"
	@test sort(Redis.get(client, ['A', 'C', 536, 'D', "STR"])) == sort(["V", "W", "str", "a", "234"])

	@test Redis.set(client, 'B', 'b', 535, 233, "STS") == "OK"
	@test sort(Redis.get(client, 'B', 535, "STS")) == sort(["b", "233", ""]) == Redis.get(client, 'B', 535, "STS", sorted=true)

	@test Redis.mset(client, [123 456]) == "OK"
	@test Redis.get(client, 123) == "456"

	@test sort(Redis.mget(client, 123, 'D', "SETMSETKEY")) == sort(["456", "W", "SETMSETVAL"]) == Redis.mget(client, 123, 'D', "SETMSETKEY", sorted=true)

	@test Redis.msetnx(client, [["MSETNXKEY" "MSETNXKEY2"], ["MSETNXVAL" "MSETNXVAL2"]]) == true
	@test Redis.msetnx(client, ["MSETNXKEY", "MSETNXVAL"]) == false
	@test Redis.msetnx(client, [['F' 'J'], ['V' 'W']]) == true
	@test Redis.msetnx(client, [321, 345]) == true
	@test sort(Redis.mget(client, 'F', "MSETNXKEY", 321, 'J')) == sort(["345", "V", "W", "MSETNXVAL"]) == Redis.mget(client, 'F', "MSETNXKEY", 321, 'J', sorted=true)

	@test Redis.exists(client, "abc") == false
	@test Redis.exists(client, "xyz") == false
	@test Redis.msetnx(client, "abc", "abc", "xyz", "xyz") == true
	@test sort(Redis.mget(client, "abc", "xyz")) == sort(["abc", "xyz"]) == Redis.mget(client, "abc", "xyz", sorted=true)
end

function test_standard_key_methods(client)
	@test Redis.set(client, "HELLO", "The quick brown fox jumped over the lazy dog.") == "OK"
	@test Redis.set(client, "INTVAL", 123) == "OK"
	@test Redis.get(client, "INTVAL") == "123"
	@test Redis.set(client, "CHARVAL", 'c') == "OK"
	@test Redis.get(client, "CHARVAL") == "c"
	@test Redis.get(client, "HELLO") == "The quick brown fox jumped over the lazy dog."
	@test Redis.getrange(client, "HELLO", 4, 8) == "quick"
	@test Redis.strlen(client, "HELLO") == 45
	@test Redis.bitcount(client, "HELLO") == 167
	@test Redis.bitcount(client, "HELLO", 0, -1) == 167
	@test Redis.bitcount(client, "HELLO", 1, 3) == 8
	@test Redis.bitpos(client, "HELLO", 1) == 1
	@test Redis.bitpos(client, "HELLO", 0) == 0
	@test Redis.bitpos(client, "HELLO", 1, 0, -1) == 1
	@test Redis.bitpos(client, "HELLO", 0, 0, -1) == 0
	@test Redis.bitpos(client, "HELLO", 1, 5, 10) == 41
	@test Redis.bitpos(client, "HELLO", 0, 5, 10) == 40
	warn("Don't have the documentation for bitop to test it atm.")
	@test Redis.getbit(client, "HELLO", 11) == 0
	@test Redis.setbit(client, "HELLO", 11, 1) == 0
	@test Redis.getbit(client, "HELLO", 11) == 1
	@test Redis.getset(client, "HELLO", "Nothing new") == "Txe quick brown fox jumped over the lazy dog."
	@test Redis.get(client, "HELLO") == "Nothing new"
	@test Redis.ttl(client, "HELLO") == -1
	@test Redis.pttl(client, "HELLO") == -1
	close(client)
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
