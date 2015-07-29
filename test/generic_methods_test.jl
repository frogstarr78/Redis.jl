using Redis
using Base.Test

include("test_client.jl")

function test_methods(client)
	Redis.flushdb(client)
	r = Redis.info(client)
	@test isa(r, String)
	@test Redis.exists(client, "KEY") == false
	r = Redis.set(client, "KEY", 2)
	@test Redis.exists(client, "KEY") == true
	@test Redis.del(client, "KEY") == true
	@test Redis.del(client, "KEY") == false
	@test Redis.exists(client, "KEY") == false
	Redis.set(client, "KEY2", "val")
	Redis.set(client, "KEY3", "val")
	@test sort(Redis.keys(client, "KEY*")) == sort(["KEY2", "KEY3"])
	@test Redis.keys(client, "*2") == ["KEY2"]
	Redis.set(client, "KEY4", "val")
	@test sort(Redis.keys(client)) == sort(["KEY2", "KEY3", "KEY4"])
	@test Redis.dbsize(client) == 3
	Redis.set(client, "KEY5", "val")
	@test Redis.dbsize(client) == 4
	r = Redis.time(client)
	@test isa(r, TmStruct)
	@test Redis.save(client) == "OK"
	@test Redis.save(client, background=false) == "OK"
	@test Redis.dbsize(client) != 0
	@test Redis.flushall(client) == "OK"
	@test Redis.dbsize(client) == 0
	Redis.set(client, "KEY4", "val")
	@test Redis.dbsize(client) != 0
	@test Redis.flushdb(client) == "OK"
	@test Redis.dbsize(client) == 0
end

function test_expire_methods(client)
	Redis.set(client, "KEY", 2)
	@test Redis.ttl(client, "KEY") == -1
	@test Redis.pttl(client, "KEY") == -1
	warn("Need to make additional tests for expire methods")
end

function test_type_methods(client)
	Redis.set(client, "KEY", 2)
	@test Redis.typeof(client, "KEY") == "string" 
	warn("Need to make additional tests for typeof method")
end

function test_long_running_bgsaves()
	test_client_with((client) -> ( println(1); @test Redis.bgsave(client) == "Background saving started"; println(1.5)))
	sleep(5)
	test_client_with((client) -> ( println(2); @test Redis.save(client, background=true) == "Background saving started"; println(2.5)))
#	@test Redis.save(client(port=9999), background=true) == "Background saving started"
	sleep(5)
	test_client_with((client) -> ( println(3); @test Redis.bgrewriteaof(client) == "Background append only file rewriting started"; println(3.5)))
#	@test Redis.bgrewriteaof(client(port=9999)) == "Background append only file rewriting started"
	close(client)
end

test_client_with(test_methods)
test_client_with(test_expire_methods)
test_client_with(test_type_methods)

##del generic
#dump generic
##exists generic
#expire generic
#expireat generic
##keys generic
#migrate generic
#move generic
#object generic
#persist generic
#pexpire generic
#pexpireat generic
##pttl generic
#randomkey generic
#rename generic
#renamenx generic
#restore generic
#scan generic
#sort generic
##ttl generic
##type generic
#wait generic
