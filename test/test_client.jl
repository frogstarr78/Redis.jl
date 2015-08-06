using Redis

function test_client_with(cb::Function)
	if isfile("/tmp/julia-test.rdb")
		rm("/tmp/julia-test.rdb")
	end
	run(`redis-server ${pwd()}/etc/redis.conf`)
	sleep(1)
	io = None
	try 
		io = client(port=9999)
		try
			cb(io)
		catch e
			error(e)
		finally
			close(io)
			open(readall, `redis-cli -p 9999 FLUSHALL`)
			run(`redis-cli -p 9999 SHUTDOWN`)
		end
	catch ee
		if isopen(io) 
			close(io)
		end
		sleep(5)
		error(ee)
	end
end

