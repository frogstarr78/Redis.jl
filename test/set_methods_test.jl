using Redis
using Base.Test

function test_methods(client)
	@test Redis.exists(client, "ASET") == false
	@test Redis.sadd(client, "ASET", "AKEY", "AVAL") == 2
	@test Redis.sadd(client, "ASET", "AKEY", "AVAL2") == 1
	@test Redis.sadd(client, "ASET", "AKEY2", "AVAL2") == 2
	@test Redis.scard(client, "ASET") == 2
	@test Redis.sismember(client, "ASET", "AKEY") == true
	@test Redis.sismember(client, "ASET", "AKEY2") == true
	@test Redis.sismember(client, "ASET", "AKEY3") == false
	@test sort(Redis.smembers(client, "ASET")) == sort() == Redis.smembers(client, "ASET", sorted=true)
end

run(`redis-server ${pwd()}/etc/redis.conf`)
try
	test_methods(client(port=9999))
catch e
	error(e)
finally
	run(`redis-cli -p 9999 SHUTDOWN`)
end
