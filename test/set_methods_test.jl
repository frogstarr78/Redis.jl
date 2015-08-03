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
	r = Redis.spop(client, "BSET")
	println(r)
	@test r == "AVAL2"
#	@test Redis.spop(client, "BSET") == "AVAL2"
	@test Redis.scard(client, "BSET") == 0
	Redis.sadd(client, "ASET", "AVAL4", "AVAL5", "AVAL6")
	@test Redis.scard(client, "ASET") == 5
	three_values = Redis.spop(client, "ASET", 3)
	@test isa(three_values, Set)
	@test length(filter(x -> x in possible_values, three_values)) == 3
	@test Redis.scard(client, "ASET") == 2

	Redis.sadd(client, "ASET", "AVAL4", "AVAL5", "AVAL6")
	@test Redis.scard(client, "ASET") == 5
	four_values = Redis.srandmember(client, "ASET", 4)
	@test isa(four_values, Array{Any,1})
	@test length(filter(x -> x in possible_values, four_values)) == 4
	@test Redis.scard(client, "ASET") == 5
	val_from_set = Redis.srandmember(client, "ASET")
	@test val_from_set in possible_values
	@test Redis.scard(client, "ASET") == 5

	@test "srem" == "unimplemented test"
	@test "#sscan" == "unimplemented test"
	@test "sunion" == "unimplemented test"
	@test "sunionstore" == "unimplemented test"
end

test_client_with(test_methods)
