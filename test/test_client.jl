using Redis

function test_clean_client_with(cb::Function)
	if isfile("/tmp/julia-test.rdb")
		rm("/tmp/julia-test.rdb")
	end
	run(`redis-server ${pwd()}/etc/redis.conf`)
	sleep(1)
	open(readall, `redis-cli -p 9999 FLUSHALL`)
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
		sleep(5)
		error(ee)
	end
end

function test_shutdown_client(cb::Function)
	if isfile("/tmp/julia-test.rdb")
		rm("/tmp/julia-test.rdb")
	end
	run(`redis-server ${pwd()}/etc/redis.conf`)
	sleep(1)
	io = client(port=9999)
	Redis.flushall(io)
	try
		cb(io)
	catch e
		error(e)
	finally
	end
end


function test_dirty_client_with(cb::Function)
	run(`redis-server ${pwd()}/etc/redis.conf`)
	sleep(1)
	io = client(port=9999)
	try
		cb(io)
	catch e
		error(e)
	finally
	end
end

