using Redis

c = client()

#rset = Redis.set(c, "hello", "world")
#println(rset)
#rget = Redis.get(c, "hello")
#println(rget)
#exs = Redis.exists(c, "moe")
#println(exs)
#rget = Redis.get(c, "joe")
#println(rget)
#rget = Redis.setnx(c, "joe", "you")
#println(rget)
#rget = Redis.get(c, "joe")
#println(rget)

p = Redis.send(c, "ping")
println("ping $p")

ex = Redis.send(c, "exists", "bob")
println("exists bob $ex")

hs = Redis.send(c, "hset", "hasssssshhhhhhhhhhh", "kee", "Val Kilmer")
println("hset hasssssshhhhhhhhhhh kee Val Kilmer = $hs")

hs = Redis.send(c, "hset", "hasssssshhhhhhhhhhh", "ankey", "3")
println("hset hasssssshhhhhhhhhhh ankey 3 = $hs")

hs = Redis.send(c, "hincrby", "hasssssshhhhhhhhhhh", "ankey", "3")
println("hincrby hasssssshhhhhhhhhhh ankey 3 = $hs")

hg = Redis.send(c, "hget", "hasssssshhhhhhhhhhh", "kee")
println("hget hasssssshhhhhhhhhhh kee = $hg")

hex = Redis.send(c, "hexists", "hasssssshhhhhhhhhhh", "this")
println("hexists hasssssshhhhhhhhhhh this = $hex")

hkee = Redis.send(c, "hkeys", "hasssssshhhhhhhhhhh")
println("hkeys hasssssshhhhhhhhhhh = $hkee")

hg = Redis.send(c, "hgetall", "hasssssshhhhhhhhhhh")
println("hgetall hasssssshhhhhhhhhhh = $hg")

sa = Redis.send(c, "sadd", "ssett", "something")
println("sadd ssett something = $sa")

sa2 = Redis.send(c, "sadd", "ssett", "something else")
println("sadd ssett something else = $sa2")

sm = Redis.send(c, "smembers", "ssett")
println(typeof(sm))
println("smembers ssett = $sm")

s = Redis.send(c, "save")
println("save $s")
#for m in hg
#	println(m)
#end

close(c)
