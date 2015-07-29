using Redis
using Base.Test

include("test_client.jl")

function test_auth_methods()
	run(`redis-server ${pwd()}/etc/redis.conf --requirepass pass`)
	try
		klient = client(port=9999, password="pass")
		@test Redis.set(klient, "key", "val") == "OK"
		close(klient)

		klient = client(port=9999)
		@test_throws Exception Redis.get(klient, "KEY")
		close(klient)

		klient = client(port=9999)
		@test Redis.auth(klient, "pass") == "OK"
		@test Redis.set(klient, "key", "val") == "OK"
		close(klient)
	catch e
		error(e)
	finally
		run(`redis-cli -a pass -p 9999 SHUTDOWN`)
	end
end

function test_methods(client)
	@test Redis.ping(client) == "PONG"
end

test_auth_methods()
test_client_with(test_methods)
