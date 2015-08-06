using Redis
using Base.Test

include("test_client.jl")

function test_methods(client)
	@test Redis.exists(client, "ASET") == false
	@test Redis.sadd(client, "ASET", "AVAL", "AVAL2") == 2
	@test Redis.sadd(client, "ASET", "AVAL2") == 0
	@test Redis.sadd(client, "ASET", "AVAL3") == 1
	@test Redis.scard(client, "ASET") == 3
	@test Redis.sismember(client, "ASET", "AVAL") == true
	@test Redis.sismember(client, "ASET", "AVAL2") == true
	@test Redis.sismember(client, "ASET", "AVAL3") == true
	@test Redis.smembers(client, "ASET") == Set(["AVAL", "AVAL2", "AVAL3"])
	@test isa(Redis.smembers(client, "BSET"), Set)
	@test Redis.scard(client, "BSET") == 0
	@test Redis.smove(client, "ASET", "BSET", "AVAL2") == 1
	@test Redis.smembers(client, "BSET") == Set(["AVAL2"])
	@test Redis.smove(client, "ASET", "BSET", "AVAL2") == 0
	@test Redis.smove(client, "ASET", "BSET", "AVAL5") == 0

	possible_values = ["AVAL6", "AVAL", "AVAL4", "AVAL3", "AVAL2", "AVAL5"]
	@test Redis.spop(client, "BSET") == "AVAL2"
	@test Redis.scard(client, "BSET") == 0
	Redis.sadd(client, "ASET", "AVAL4", "AVAL5", "AVAL6")
	@test Redis.scard(client, "ASET") == 5
	three_values = Redis.spop(client, "ASET", 3)
	@test isa(three_values, Set)
	@test length(filter(x -> x in possible_values, three_values)) == 3
	@test Redis.scard(client, "ASET") == 2

	push!(possible_values, "AVAL7", "AVAL8", "AVAL9")
	Redis.sadd(client, "ASET", "AVAL7", "AVAL8", "AVAL9")
	@test Redis.scard(client, "ASET") == 5
	four_values = Redis.srandmember(client, "ASET", 4)
	@test isa(four_values, Set)
	@test length(filter(x -> x in possible_values, four_values)) == 4

	@test Redis.scard(client, "ASET") == 5
	val_from_set = Redis.srandmember(client, "ASET")
	@test val_from_set in possible_values

	@test Redis.scard(client, "ASET") == 5

	@test Redis.srem(client, "ASET", "AVAL7") == 1
	@test Redis.srem(client, "ASET", "AVAL7", "AVAL9") == 1
	Redis.sadd(client, "ASET", possible_values...)
	@test Redis.srem(client, "ASET", "AVAL", "AVAL9", "AVAL4") == 3
	map(val -> Redis.srem(client, "ASET", val), possible_values)

	Redis.sadd(client, "ASET", "AVAL", "AVAL2", "AVAL3")
	Redis.sadd(client, "BSET", "BVAL", "BVAL2")
	@test Redis.sunion(client, "ASET", "BSET") == Set(["AVAL", "AVAL2", "AVAL3", "BVAL", "BVAL2"])
	Redis.sadd(client, "CSET", "CVAL", "CVAL2")
	@test Redis.sunion(client, "ASET", "BSET", "CSET") == Set(["AVAL", "AVAL2", "AVAL3", "BVAL", "BVAL2", "CVAL", "CVAL2"])

	@test Redis.sunionstore(client, "DESTSET", "ASET", "CSET") == 5
	@test Redis.sunionstore(client, "DESTSET", "CSET") == 2
	@test Redis.sunionstore(client, "DESTSET", "ASET", "CSET", "BSET") == 7

end

function test_scan_method(client)
	warn("sscan untested")
end

test_clean_client_with(test_methods)
test_clean_client_with(test_scan_method)
