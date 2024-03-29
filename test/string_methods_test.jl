using Redis
using Base.Test

include("test_client.jl")

function test_append_methods(client)
	@test Redis.get(client, "KEYAPPEND") == ""
	@test Redis.append(client, "KEYAPPEND", "VAL") == 3
	@test Redis.get(client, "KEYAPPEND") == "VAL"
end

function test_incr_methods(client)
	@test Redis.incr(client, "KEY") == 1
	@test Redis.incrby(client, "KEY", 4) == 5
	@test_approx_eq Redis.incrby(client, "KEY", 4.1) 9.1
	@test_approx_eq Redis.incrbyfloat(client, "KEY", 3.2) 12.3
end

function test_decr_methods(client)
	@test Redis.set(client, "KEY", 12) == "OK"
	@test Redis.decr(client, "KEY") == 11
	@test Redis.decrby(client, "KEY", 4) == 7

	@test_approx_eq Redis.decrby(client, "KEY", 4.1) 2.9
	@test Redis.decrbyfloat(client, "KEY", 2.9) == 0
end

function test_mset_methods(client)
	@test Redis.mset(client, [["MSETKEY" "MSETKEY2"], ["VAL" "VAL2"]]::Array{ASCIIString,2}) == "OK"
	@test "VAL" in Redis.mget(client, ["MSETKEY", "MSETKEY2"])
	@test "VAL2" in Redis.mget(client, ["MSETKEY", "MSETKEY2"])
	@test sort(Redis.mget(client, ["MSETKEY", "MSETKEY2"])) == sort(["VAL", "VAL2"])
	@test sort(Redis.mget(client, ["MSETKEY", "MSETKEY2"])) == Redis.mget(client, ["MSETKEY", "MSETKEY2"], sorted=true)

#	@test Redis.set(client, ["SETMSETKEY" "SETMSETVAL"]) == "OK"
#	@test Redis.get(client, "SETMSETKEY") == "SETMSETVAL"
#
#	@test sort(Redis.mget(client, "SETMSETKEY", "MSETKEY")) == sort(["VAL", "SETMSETVAL"]) == Redis.mget(client, "SETMSETKEY", "MSETKEY", sorted=true)
#
#	@test Redis.mset(client, ['C' 'V']) == "OK"
#	@test Redis.get(client, 'C') == "V"
#
#	@test Redis.set(client, ['D' 'W']) == "OK"
#	@test Redis.get(client, 'D') == "W"
#
#	@test Redis.set(client, ['A', 'a', 536, 234, "STR", "str"]) == "OK"
#	@test sort(Redis.get(client, ['A', 'C', 536, 'D', "STR"])) == sort(["V", "W", "str", "a", "234"])
#
#	@test Redis.set(client, 'B', 'b', 535, 233, "STS") == "OK"
#	@test sort(Redis.get(client, 'B', 535, "STS")) == sort(["b", "233", ""]) == Redis.get(client, 'B', 535, "STS", sorted=true)
#
#	@test Redis.mset(client, [123 456]) == "OK"
#	@test Redis.get(client, 123) == "456"
#
#	@test sort(Redis.mget(client, 123, 'D', "SETMSETKEY")) == sort(["456", "W", "SETMSETVAL"]) == Redis.mget(client, 123, 'D', "SETMSETKEY", sorted=true)
#
#	@test Redis.msetnx(client, [["MSETNXKEY" "MSETNXKEY2"], ["MSETNXVAL" "MSETNXVAL2"]]) == true
#	@test Redis.msetnx(client, ["MSETNXKEY", "MSETNXVAL"]) == false
#	@test Redis.msetnx(client, [['F' 'J'], ['V' 'W']]) == true
#	@test Redis.msetnx(client, [321, 345]) == true
#	@test sort(Redis.mget(client, 'F', "MSETNXKEY", 321, 'J')) == sort(["345", "V", "W", "MSETNXVAL"]) == Redis.mget(client, 'F', "MSETNXKEY", 321, 'J', sorted=true)
#
#	@test Redis.exists(client, "abc") == false
#	@test Redis.exists(client, "xyz") == false
#	@test Redis.msetnx(client, "abc", "abc", "xyz", "xyz") == true
#	@test sort(Redis.mget(client, "abc", "xyz")) == sort(["abc", "xyz"]) == Redis.mget(client, "abc", "xyz", sorted=true)
end

function test_string_methods(client)
	@test Redis.set(client, "HELLO", "The quick brown fox jumped over the lazy dog.") == "OK"
	@test Redis.set(client, "INTVAL", 123) == "OK"
	@test Redis.get(client, "INTVAL") == "123"
	@test Redis.set(client, "CHARVAL", 'c') == "OK"
	@test Redis.get(client, "CHARVAL") == "c"
	@test Redis.get(client, "HELLO") == "The quick brown fox jumped over the lazy dog."
	@test Redis.getrange(client, "HELLO", 4, 8) == "quick"
	@test Redis.setrange(client, "HELLO", 4, "dumb") == 45
	@test Redis.get(client, "HELLO") == "The dumbk brown fox jumped over the lazy dog."
	@test Redis.strlen(client, "HELLO") == 45
	@test Redis.bitcount(client, "HELLO") == 166
	@test Redis.bitcount(client, "HELLO", 0, -1) == 166
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
	@test Redis.getset(client, "HELLO", "Nothing new") == "Txe dumbk brown fox jumped over the lazy dog."
	@test Redis.get(client, "HELLO") == "Nothing new"
end

function test_scan_method(client)
	warn("Scan untested")
end

test_clean_client_with(test_append_methods)
test_clean_client_with(test_incr_methods)
test_clean_client_with(test_decr_methods)
test_clean_client_with(test_mset_methods)
test_clean_client_with(test_string_methods)
