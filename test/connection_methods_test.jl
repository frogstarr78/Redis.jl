using Redis
using Base.Test

include("test_client.jl")

function test_auth_methods()
	run(`redis-server $[pwd()]/etc/redis.conf --requirepass pass`)
	try
		klient = client(port=9999, password="pass")
		@test Redis.set(klient, "key", "val") == "OK"
		@test isopen(klient)
		@test Redis.quit(klient) == "OK"
		sleep(3)
		@test !isopen(klient)

		klient = client(port=9999)
		@test_throws Exception Redis.get(klient, "KEY")
		close(klient)

		klient = client(port=9999)
		@test Redis.auth(klient, "pass") == "OK"
		@test Redis.set(klient, "key", "val") == "OK"
		close(klient)
	catch e
		throw(e)
	finally
		run(`redis-cli -a pass -p 9999 SHUTDOWN`)
	end
end

function test_methods(client)
	@test Redis.ping(client) == "PONG"
	@test Redis.echo(client, "Hello World") == "Hello World"
	@test isopen(client)
	Redis.quit(client)
	sleep(3)
	@test !isopen(client)
end

function test_database_select(client)
	Redis.set(client, "KEY", "VAL")
	@test Redis.dbsize(client) == 1
	@test Redis.select!(client, 1) == "OK"
	@test Redis.dbsize(client) == 0
	@test Redis.select!(client, 0) == "OK"
	@test Redis.dbsize(client) == 1
	@test Redis.move(client, "KEY", 1)
	@test Redis.dbsize(client) == 0
	@test Redis.select!(client, 1) == "OK"
	@test Redis.dbsize(client) == 1
end

function test_select_on_connect()
	run(`redis-server $[pwd()]/etc/redis.conf`)
	sleep(1)
	io = Union
	try 
		try
			io = client(port=9999, db=1)
			Redis.flushall(io)
			@test Redis.dbsize(io) == 0
			Redis.set(io, "KEY", "VAL")
			@test Redis.dbsize(io) == 1
			@test Redis.select!(io, 0) == "OK"
			@test Redis.dbsize(io) == 0
		catch e
			throw(e)
		finally
			close(io)
			run(`redis-cli -p 9999 SHUTDOWN`)
		end
	catch ee
		isopen(io) || close(io)
		throw(ee)
	end
end

function test_rename_commands(client)
	Redis.flushall(client)
	Redis.set(client, "KEY", "VAL")
	@test Redis.exists(client, "KEY") == 1
	@test Redis.exists(client, "NEWKEY") == 0
	@test Redis.rename(client, "KEY", "NEWKEY") == "OK"
	@test Redis.exists(client, "KEY") == 0
	@test Redis.exists(client, "NEWKEY") == 1

	@test Redis.renamenx(client, "NEWKEY", "KEY") == "OK"
	@test_throws Exception Redis.renamenx(client, "KEY", "KEY")
	@test_throws Exception Redis.rename(client, "KEY", "KEY", not_exists=true)
	@test Redis.rename(client, "KEY", "NKEY", not_exists=true) == "OK"
end

test_auth_methods()
test_clean_client_with(test_methods)
test_clean_client_with(test_database_select)
test_clean_client_with(test_rename_commands)
test_select_on_connect()
