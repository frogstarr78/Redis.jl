using Redis
using Base.Test

include("test_client.jl")

function test_methods(client)
	@test Redis.exists(client, "KEY") == 0
	r = Redis.set(client, "KEY", 2)
	@test Redis.exists(client, "KEY") == 1
	@test Redis.del(client, "KEY") == true
	@test Redis.del(client, "KEY") == false
	@test Redis.exists(client, "KEY") == 0
	Redis.set(client, "KEY2", "val")
	Redis.set(client, "KEY3", "val")
	@test sort(Redis.keys(client, "KEY*")) == sort(["KEY2", "KEY3"])
	@test Redis.keys(client, "*2") == ["KEY2"]
	Redis.set(client, "KEY4", "val")
	@test sort(Redis.keys(client)) == sort(["KEY2", "KEY3", "KEY4"])
end

function test_expire_methods(client)
	Redis.set(client, "KEY", 2)
	@test Redis.ttl(client, "KEY") == -1
	@test Redis.pttl(client, "KEY") == -1

	expiry = 3
	@test Redis.expire(client, "KEY", expiry) == true
	sleep(expiry)
	@test Redis.exists(client, "KEY") == 0

	Redis.set(client, "KEY", 2)
	@test Redis.expire(client, "KEY", expiry) == true
	@test Redis.persist(client, "KEY") == true
	sleep(expiry)
	@test Redis.exists(client, "KEY") == 1
	@test Redis.ttl(client, "KEY") == -1
	@test Redis.exists(client, "KEY", "KEY", "KEY") == 3
	warn("Need to make additional tests for expire methods")
end

function test_type_methods(client)
	Redis.set(client, "KEY", 2)
	@test Redis.typeof(client, "KEY") == "string" 
	Redis.sadd(client, "SET", "VAL")
	@test Redis.typeof(client, "SET") == "set" 
	Redis.hset(client, "HASH", "FIELD", "VAL")
	@test Redis.typeof(client, "HASH") == "hash" 
	warn("Need to make additional tests for typeof method")
end

test_clean_client_with(test_methods)
test_clean_client_with(test_expire_methods)
test_clean_client_with(test_type_methods)
