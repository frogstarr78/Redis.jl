using Redis
using Base.Test

io = IOBuffer(true, true)
r =  Redis.send(io, "SET", "KEY", "VALUE")
@test r == ["SET", "KEY", "VALUE"]

io = IOBuffer(true, true)
r =  Redis.set(io, "KEY", "VALUE")
@test r == ["SET", "KEY", "VALUE"]

io = IOBuffer(true, true)
@test_throws Exception Redis.set(io, "KEY", "VALUE", not_exists=true, if_exists=true)

io = IOBuffer(true, true)
r =  Redis.set(io, "KEY", "VALUE", sec_expire=3)
@test typeof(r) == typeof({})
@test r == ["SET", "KEY", "VALUE", "EX", "3"]

io = IOBuffer(true, true)
r =  Redis.set(io, "KEY", "VALUE", sec_expire=3, ms_expire=4, not_exists=true)
@test typeof(r) == typeof({})
@test r == ["SET", "KEY", "VALUE", "EX", "3", "PX", "4", "NX"]

io = IOBuffer(true, true)
r =  Redis.set(io, "KEY", "VALUE", sec_expire=3, ms_expire=4, if_exists=true)
@test typeof(r) == typeof({})
@test r == ["SET", "KEY", "VALUE", "EX", "3", "PX", "4", "XX"]

io = IOBuffer(true, true)
r =  Redis.setnx(io, "KEY", "VALUE")
@test typeof(r) == typeof({})
@test r == ["SET", "KEY", "VALUE", "NX"]

io = IOBuffer(true, true)
r =  Redis.setex(io, "KEY", 5, "VALUE")
@test typeof(r) == typeof({})
@test r == ["SET", "KEY", "VALUE", "EX", "5"]

io = IOBuffer(true, true)
r =  Redis.psetex(io, "KEY", 7, "VALUE")
@test typeof(r) == typeof({})
@test r == ["SET", "KEY", "VALUE", "PX", "7"]

io = IOBuffer(true, true)
r =  Redis.set(io, "KEY", '1')
@test r == ["SET", "KEY", "1"]

io = IOBuffer(true, true)
r =  Redis.set(io, "KEY", 5)
@test r == ["SET", "KEY", "5"]

io = IOBuffer(true, true)
r =  Redis.del(io, "KEY")
@test r == ["DEL", "KEY"]

io = IOBuffer(true, true)
r =  Redis.del(io, "KEY", "KEY2")
@test r == ["DEL", "KEY", "KEY2"]

io = IOBuffer(true, true)
r =  Redis.del(io, "KEY", "KEY2", "KEY")
@test r == ["DEL", "KEY", "KEY2", "KEY"]

io = IOBuffer(true, true)
r =  Redis.get(io, "KEY")
@test r == ["GET", "KEY"]

io = IOBuffer(true, true)
r =  Redis.mget(io, ["KEY", "KEY2"])
@test r == ["MGET", "KEY", "KEY2"]

io = IOBuffer(true, true)
r =  Redis.mget(io, "KEY", "KEY2")
@test r == ["MGET", "KEY", "KEY2"]

