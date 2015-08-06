using Redis
using Base.Test

include("test_client.jl")

function test_long_running_bgsave(client)
	@test Redis.bgsave(client) == "Background saving started"
end

function test_long_running_save_background(client)
	@test Redis.save(client, background=true) == "Background saving started"
end

function test_long_save(client)
	@test Redis.save(client, background=false) == "OK"
end

function test_long_running_bgrewriteaof(client)
	@test Redis.bgrewriteaof(client) == "Background append only file rewriting started"
end

function test_methods(client)
	@test Redis.dbsize(client) == 0
	Redis.flushdb(client)
	@test Redis.dbsize(client) == 0
	Redis.set(client, "KEY", "VAL")
	Redis.set(client, "KEY2", "VAL2")
	Redis.set(client, "KEY3", "VAL3")
	@test Redis.save(client) == "OK"
	@test Redis.dbsize(client) == 3
	@test Redis.flushall(client) == "OK"
	@test Redis.dbsize(client) == 0
	Redis.set(client, "KEY", "VAL")
	Redis.set(client, "KEY2", "VAL2")
	Redis.set(client, "KEY3", "VAL3")
	Redis.set(client, "KEY4", "VAL4")
	@test Redis.dbsize(client) == 4
	@test Redis.flushdb(client) == "OK"
	@test Redis.dbsize(client) == 0
	r = Redis.info(client)
	@test isa(r, String)
	@test contains(r, "redis_version")
	@test contains(r, "connected_clients:1")
	@test r[1:8] == "# Server"

	r = Redis.info(client, "server")
	@test isa(r, String)
	@test contains(r, "redis_version")
	@test contains(r, "connected_clients:1") == false
	@test r[1:8] == "# Server"

	r = Redis.info(client, "clients")
	@test isa(r, String)
	@test r[1:8] == "# Client"

	r = Redis.info(client, "keyspace")
	@test isa(r, String)
	@test contains(r, "redis_version") == false
	@test r[1:10] == "# Keyspace"

	@test_throws Exception Redis.info(client, "nonsection")

	r = Redis.time(client)
	@test isa(r, TmStruct)
end

function test_shutdown_methods()
	test_shutdown_client() do client
		Redis.set(client, "KEY", "VAL")
		Redis.set(client, "KEY2", "VAL2")
		Redis.set(client, "KEY3", "VAL3")
		Redis.set(client, "KEY4", "VAL4")
		@test Redis.dbsize(client) == 4

		Redis.shutdown(client)
		@test isopen(client) == false
		open(`netstat -lnt`) do procss
			r = readall(procss)
			@test contains(r, "9999") == false
		end
	end
	test_dirty_client_with(c -> @test Redis.dbsize(c) == 0)

	test_shutdown_client() do client
		Redis.set(client, "KEY", "VAL")
		Redis.set(client, "KEY2", "VAL2")
		Redis.set(client, "KEY3", "VAL3")
		Redis.set(client, "KEY4", "VAL4")
		@test Redis.dbsize(client) == 4

		Redis.shutdown(client, "save")
		open(`netstat -lnt`) do procss
			r = readall(procss)
			@test contains(r, "9999") == false
		end
	end
	test_dirty_client_with(c -> @test Redis.dbsize(c) == 4)

	test_shutdown_client() do client
		Redis.flushall(client)
		Redis.set(client, "KEY", "VAL")
		Redis.set(client, "KEY2", "VAL2")
		Redis.set(client, "KEY3", "VAL3")
		Redis.set(client, "KEY4", "VAL4")

		Redis.shutdown(client, "nosave")
		open(`netstat -lnt`) do procss
			r = readall(procss)
			@test contains(r, "9999") == false
		end
	end
	test_dirty_client_with(c -> @test Redis.dbsize(c) == 0)

	test_shutdown_client() do client
		Redis.flushall(client)
		Redis.set(client, "KEY", "VAL")
		Redis.set(client, "KEY2", "VAL2")
		Redis.set(client, "KEY3", "VAL3")
		Redis.set(client, "KEY4", "VAL4")

		Redis.shutdown(client, true)
		open(`netstat -lnt`) do procss
			r = readall(procss)
			@test contains(r, "9999") == false
		end
	end
	test_dirty_client_with(c -> @test Redis.dbsize(c) == 4)

	test_shutdown_client() do client
		Redis.flushall(client)
		Redis.set(client, "KEY", "VAL")
		Redis.set(client, "KEY2", "VAL2")
		Redis.set(client, "KEY3", "VAL3")
		Redis.set(client, "KEY4", "VAL4")

		Redis.shutdown(client, false)
		open(`netstat -lnt`) do procss
			r = readall(procss)
			@test contains(r, "9999") == false
		end
	end
	test_dirty_client_with(c -> @test Redis.dbsize(c) == 0)
end

test_clean_client_with(test_methods)
warn("These involve time specific commands so they may take some time")
test_clean_client_with(test_long_running_bgsave)
test_clean_client_with(test_long_running_save_background)
test_clean_client_with(test_long_save)
test_clean_client_with(test_long_running_bgrewriteaof)
test_shutdown_methods()
