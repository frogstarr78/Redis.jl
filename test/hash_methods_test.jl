using Redis
using Base.Test

function test_methods(io)
end

run(`redis-server ${pwd()}/etc/redis.conf`)
try
	test_methods(client(port=9999))
catch e
	error(e)
finally
	run(`redis-cli -p 9999 SHUTDOWN`)
end
