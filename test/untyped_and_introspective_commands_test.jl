using Redis
using Base.Test


function test_methods(io)
	@test Redis.ping(io) == "PONG"
	Redis.flushdb(io)
	r = Redis.info(io)
	@test isa(r, String)
	@test Redis.exists(io, "KEY") == false
	r = Redis.set(io, "KEY", 2)
	@test Redis.typeof(io, "KEY") == "string" 
	@test Redis.exists(io, "KEY") == true
	@test Redis.del(io, "KEY") == true
	@test Redis.del(io, "KEY") == false
	@test Redis.exists(io, "KEY") == false
	Redis.set(io, "KEY2", "val")
	Redis.set(io, "KEY3", "val")
	@test Redis.keys(io, "KEY*") == ["KEY2", "KEY3"]
	@test Redis.keys(io, "*2") == ["KEY2"]
	Redis.set(io, "KEY4", "val")
	@test sort(Redis.keys(io)) == sort(["KEY2", "KEY3", "KEY4"])
	@test Redis.dbsize(io) == 3
	Redis.set(io, "KEY5", "val")
	@test Redis.dbsize(io) == 4
	r = Redis.time(io)
	@test isa(r, TmStruct)
	@test Redis.save(io) == "OK"
	@test Redis.save(io, background=false) == "OK"
	@test Redis.dbsize(io) != 0
	@test Redis.flushall(io) == "OK"
	@test Redis.dbsize(io) == 0
	Redis.set(io, "KEY4", "val")
	@test Redis.dbsize(io) != 0
	@test Redis.flushdb(io) == "OK"
	@test Redis.dbsize(io) == 0
	@test Redis.bgsave(io) == "Background saving started"
	sleep(3)
	@test Redis.save(client(port=9999), background=true) == "Background saving started"
	sleep(3)
	@test Redis.bgrewriteaof(client(port=9999)) == "Background append only file rewriting started"
end

function test_auth_methods()
	run(`redis-server ${pwd()}/etc/redis.conf --requirepass pass`)
	try
		io = client(port=9999, password="pass")
		@test Redis.set(io, "key", "val") == "OK"
		close(io)

		io = client(port=9999)
		@test_throws Exception Redis.get(io, "KEY")
		close(io)

		io = client(port=9999)
		@test Redis.auth(io, "pass") == "OK"
		@test Redis.set(io, "key", "val") == "OK"
		close(io)
	catch e
		error(e)
	finally
		run(`redis-cli -a pass -p 9999 SHUTDOWN`)
	end
end

run(`redis-server ${pwd()}/etc/redis.conf`)
try
	test_methods(client(port=9999))
catch e
	error(e)
finally
	run(`redis-cli -p 9999 SHUTDOWN`)
end

test_auth_methods()
