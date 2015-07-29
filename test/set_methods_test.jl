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
	@test Redis.smove(client, "ASET", "BSET", "AVAL2") == 1
	@test Redis.smove(client, "ASET", "BSET", "AVAL2") == 0
	@test Redis.smove(client, "ASET", "BSET", "AVAL5") == 0
	@test "spop" == "unimplemented test"
	@test "srandmember" == "unimplemented test"
	@test "srem" == "unimplemented test"
	@test "#sscan" == "unimplemented test"
	@test "sunion" == "unimplemented test"
	@test "sunionstore" == "unimplemented test"
end

test_client_with(test_methods)
