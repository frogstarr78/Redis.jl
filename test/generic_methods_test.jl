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
end

function test_expireat_method(client)
	Redis.set(client, "KEY", 2)
	ttl = 3
	when = parseint(strftime("%s", time()))+ttl
	@test Redis.expireat(client, "KEY", when)
	@test Redis.ttl(client, "KEY") <= ttl
	sleep(ttl)
	@test Redis.exists(client, "KEY") == 0
end

function test_pexpire_methods(client)
	Redis.set(client, "KEY", 2)
	@test Redis.exists(client, "KEY") == true
	@test Redis.pexpire(client, "KEY", 400) == true
	@test Redis.ttl(client, "KEY") == 0
	@test Redis.pttl(client, "KEY") <= 400
	sleep(0.41)
	@test Redis.exists(client, "KEY") == false
end

function test_pexpireat_methods(client)
	Redis.set(client, "KEY", 2)
	ttl = 400
	when = parseint(string(strftime("%s", time()), ttl))
	@test Redis.expireat(client, "KEY", when)
	@printf "pttl '%s', when '%s'\n" Redis.pttl(client, "KEY") when
	@test Redis.pttl(client, "KEY") <= when
	sleep(0.41)
	@test Redis.exists(client, "KEY") == 0
end

function test_type_methods(client)
	Redis.set(client, "KEY", 2)
	@test Redis.typeof(client, "KEY") == "string" 
	Redis.sadd(client, "SET", "VAL")
	@test Redis.typeof(client, "SET") == "set" 
	Redis.hset(client, "HASH", "FIELD", "VAL")
	@test Redis.typeof(client, "HASH") == "hash" 
	Redis.lpush(client, "LIST", "VAL")
	@test Redis.typeof(client, "LIST") == "list" 

	warn("Need to make additional tests for typeof method")
end

function test_random(client)
	Redis.set(client, "KEY", "sval")
	Redis.lpush(client, "LKEY", "lkey")
	Redis.sadd(client, "SKEY", "setval")
	Redis.hset(client, "HKEY", "hfield", "hval")

	r = Redis.randomkey(client)
	@test r in ["KEY", "LKEY", "SKEY", "HKEY"]
end

test_clean_client_with(test_methods)
test_clean_client_with(test_expire_methods)
test_clean_client_with(test_expireat_method)
test_clean_client_with(test_pexpire_methods)
#test_clean_client_with(test_pexpireat_methods)
test_clean_client_with(test_type_methods)
test_clean_client_with(test_random)
