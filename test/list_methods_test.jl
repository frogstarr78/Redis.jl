using Redis
using Base.Test

include("test_client.jl")

function test_methods(client)
	@test Redis.lpush(client, "LISTK", "LVAL") == 1 == Redis.llen(client, "LISTK")
	@test Redis.lpush(client, "LISTK", "LVAL2") == 2 == Redis.llen(client, "LISTK")
	@test Redis.lindex(client, "LISTK", 0) == "LVAL2"
	@test Redis.lindex(client, "LISTK", 1) == "LVAL"
	@test Redis.lindex(client, "LISTK", -1) == "LVAL"
	@test Redis.rpush(client, "LISTK", "LVAL3") == 3 == Redis.llen(client, "LISTK")
	@test Redis.lindex(client, "LISTK", -1) == "LVAL3"
	@test Redis.lpop(client, "LISTK") == "LVAL2"
	@test Redis.llen(client, "LISTK") == 2
	@test Redis.rpop(client, "LISTK") == "LVAL3"
	@test Redis.llen(client, "LISTK") == 1
	@test_throws Exception Redis.lset(client, "LISTK", 1, "LVAL4")
	@test Redis.rpush(client, "LISTK", "LVAL3") == 2
	@test Redis.lset(client, "LISTK", 1, "LVAL4") == "OK"
	@test Redis.lset(client, "LISTK", 1, "LVAL5") == "OK"
	@test Redis.llen(client, "LISTK") == 2
	@test Redis.lindex(client, "LISTK", 1) == "LVAL5"
	@test_throws Exception Redis.linsert(client, "LISTK", "ANYWHERE", "LVAL8", "LVAL9")
	@test Redis.linsert(client, "LISTK", "BEFORE", "LVAL5", "LVAL6") == 3
	@test Redis.linsert(client, "LISTK", "after", "LVAL6", "LVAL7") == 4
	@test Redis.linsert(client, "LISTK", "AFTER", "LVAL8", "LVAL9") == -1
	@test Redis.llen(client, "LISTK") == 4
	@test Redis.lrem(client, "LISTK", 0, "LVAL7") == 1
	@test Redis.lrange(client, "LISTK", 0, -1) == ["LVAL", "LVAL6", "LVAL5"]
	@test Redis.lrange(client, "LISTK", 1, 2) == ["LVAL6", "LVAL5"]
	@test Redis.ltrim(client, "LISTK", 1, 2) == "OK"
	@test Redis.lrange(client, "LISTK", 0, -1) == ["LVAL6", "LVAL5"]
	@test Redis.llen(client, "LISTK") == 2
	
	@test Redis.ltrim(client, "LISTK", 1, 0) == "OK"
	@test Redis.llen(client, "LISTK") == 0
	@test Redis.lpush(client, "LISTK", "one", "two", "three") == 3
	@test Redis.lpush(client, "LISTK", ["seven", "eight"]) == 5
	@test Redis.rpush(client, "LISTK", ["six", "five", "four"]) == 8
	@test Redis.rpush(client, "LISTK", "nine", "ten") == 10
	@test Redis.lrange(client, "LISTK", 0, -1) == ["eight", "seven", "three", "two", "one", "six", "five", "four", "nine", "ten"]
	@test Redis.lrem(client, "LISTK", -3, "eleven") == 0
	@test Redis.lrem(client, "LISTK", -3, "four") == 1
	@test Redis.lrem(client, "LISTK", 3, "zero") == 0
	@test Redis.lrem(client, "LISTK", 3, "two") == 1
	@test Redis.llen(client, "LISTK") == 8
	@test Redis.rpush(client, "LISTK", "eleven", "eleven", "eleven") == 11
	@test Redis.lrem(client, "LISTK", -2, "eleven") == 2
	@test Redis.lindex(client, "LISTK", -1) == "eleven"
	@test Redis.lpush(client, "LISTK", "zero", "zero", "zero", "zero") == 13
	@test Redis.lrem(client, "LISTK", 3, "zero") == 3
	@test Redis.lindex(client, "LISTK", 0) == "zero"
	@test Redis.llen(client, "LISTK") == 10

	@test Redis.rpush(client, "LKEE", "one", "two", "three") == 3
	@test Redis.rpush(client, "LKEE2", "four", "five", "six") == 3
	@test Redis.rpoplpush(client, "LKEE", "LKEE2") == "three"
	@test Redis.lrange(client, "LKEE", 0, -1) == ["one", "two"]
	@test Redis.lrange(client, "LKEE2", 0, -1) == ["three", "four", "five", "six"]
	@test Redis.rpoplpush(client, "LKEE2", "LKEE2") == "six"
	@test Redis.lrange(client, "LKEE2", 0, -1) == ["six", "three", "four", "five"]
	@test Redis.rpoplpush(client, "LKEE2", "LKEE2") == "five"
	@test Redis.lrange(client, "LKEE2", 0, -1) == ["five", "six", "three", "four"]

	@test Redis.rpush(client, "LK", 1, 3, 4) == 3
	@test Redis.lpush(client, "LK", 'a', 'c', 'b', 'e') == 7
	@test Redis.lpush(client, "LK", 1.2, 3.4) == 9
	@test Redis.rpush(client, "LK", true, false) == 11

	@test Redis.lpushx(client, "LKEE3", "abc") == 0
	@test Redis.lpush(client, "LKEE3", "abc", "def") == 2
	@test Redis.lpushx(client, "LKEE3", "hij") == 3
	@test Redis.del(client, "LKEE3") == true

	@test Redis.lpush(client, "LKEE3", "abc", if_exists=true) == 0
	@test Redis.lpush(client, "LKEE3", "abc", "def") == 2
	@test Redis.lpush(client, "LKEE3", "hij", if_exists=true) == 3
	@test Redis.del(client, "LKEE3") == true

	@test Redis.rpushx(client, "LKEE3", "abc") == 0
	@test Redis.rpush(client, "LKEE3", "abc", "def") == 2
	@test Redis.rpushx(client, "LKEE3", "hij") == 3
	@test Redis.del(client, "LKEE3") == true

	@test Redis.rpush(client, "LKEE3", "abc", if_exists=true) == 0
	@test Redis.rpush(client, "LKEE3", "abc", "def") == 2
	@test Redis.rpush(client, "LKEE3", "hij", if_exists=true) == 3
	@test Redis.del(client, "LKEE3") == true
end

function test_client_only_methods(client)
	@test Redis.lpush(client, "KEE", "ONE", "TWO", "THREE", "FOUR", "FIVE") == 5 == Redis.llen(client, "KEE")
	@test Redis.lrange(client, "KEE") == ["FIVE", "FOUR", "THREE", "TWO", "ONE"]
	@test Redis.rpop(client, "KEE", 3) == ["ONE", "TWO", "THREE"]
	@test Redis.rpush(client, "KEE", ["ONE", "TWO", "THREE"]) == 5 == Redis.llen(client, "KEE")
	@test Redis.lrange(client, "KEE") == ["FIVE", "FOUR", "ONE", "TWO", "THREE"]
	@test Redis.lpop(client, "KEE", 3) == ["FIVE", "FOUR", "ONE"]

	@test Redis.lpushx(client, "LKEE3", "abc", "def") == 0
	@test Redis.lpush(client, "LKEE3", "abc", "def") == 2
	@test Redis.lpushx(client, "LKEE3", "hij", "xyz") == 4
	@test Redis.del(client, "LKEE3") == true

	@test Redis.lpush(client, "LKEE3", "abc", "def", if_exists=true) == 0
	@test Redis.lpush(client, "LKEE3", "abc", "def") == 2
	@test Redis.lpush(client, "LKEE3", "hij", "xyz", if_exists=true) == 4
	@test Redis.del(client, "LKEE3") == true

	@test Redis.rpushx(client, "LKEE3", "abc") == 0
	@test Redis.rpush(client, "LKEE3", "abc", "def") == 2
	@test Redis.rpushx(client, "LKEE3", "hij", "xyz") == 4
	@test Redis.del(client, "LKEE3") == true

	@test Redis.rpush(client, "LKEE3", "abc", if_exists=true) == 0
	@test Redis.rpush(client, "LKEE3", "abc", "def") == 2
	@test Redis.rpush(client, "LKEE3", "hij", "xyz", if_exists=true) == 4
	@test Redis.del(client, "LKEE3") == true
end

function test_atomic_methods(client)
	warn("Haven't tested atomic methods yet")
end

test_clean_client_with(test_methods)
test_clean_client_with(test_client_only_methods)
test_clean_client_with(test_atomic_methods)
