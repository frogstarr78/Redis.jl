using Redis

function test_client_with(cb::Function)
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
			run(`redis-cli -p 9999 SHUTDOWN`)
		end
	catch ee
		if isopen(io) 
			close(io)
		end
		error(ee)
	end
end

