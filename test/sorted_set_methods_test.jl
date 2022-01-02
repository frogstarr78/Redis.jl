using Redis
using Base.Test

include("test_client.jl")

function test_methods(client)
	@test_throws Exception Redis.zadd(client, "ZKEY", 1, "VAL", if_exists=true, not_exists=true)
	@test Redis.exists(client, "ZKEY") == 0
	@test Redis.zadd(client,   "ZKEY", 1.1, "ZVAL") == 1
	@test Redis.zscore(client, "ZKEY", "ZVAL") ≈ 1.1
	@test Redis.zrank(client,  "ZKEY", "ZVAL") == 0
	@test Redis.zcount(client,  "ZKEY", 1, 3) == 1
	@test Redis.zcard(client,  "ZKEY") == 1
	@test Redis.zadd(client,   "ZKEY", 2.1, "ZVAL2") == 1
	@test Redis.zscore(client, "ZKEY", "ZVAL2") ≈ 2.1
	@test Redis.zrank(client,  "ZKEY", "ZVAL2") == 1
	@test Redis.zcount(client,  "ZKEY", 1, 3) == 2
	@test Redis.zcard(client,  "ZKEY") == 2
	@test Redis.zadd(client,   "ZKEY", "NX", 3.0, "ZVAL3") == 1
	@test Redis.zscore(client, "ZKEY", "ZVAL3") ≈ 3.0
	@test Redis.zrank(client,  "ZKEY", "ZVAL3") == 2
	@test Redis.zcount(client,  "ZKEY", 1.0, 4.0) == 3
	@test Redis.zcard(client,  "ZKEY") == 3
	@test Redis.zadd(client,   "ZKEY", "XX", 3.0, "ZVAL3") == 0
	@test Redis.zscore(client, "ZKEY", "ZVAL3") ≈ 3.0
	@test Redis.zrank(client,  "ZKEY", "ZVAL3") == 2
	@test Redis.zcount(client,  "ZKEY", 1.0, 4) == 3
	@test Redis.zcard(client,  "ZKEY") == 3
	@test Redis.zadd(client,   "ZKEY", "XX", "CH", 3.0, "ZVAL3") == 0
	@test Redis.zadd(client,   "ZKEY", "NX", "CH", 3.0, "ZVAL3") == 0
	@test Redis.zscore(client, "ZKEY", "ZVAL3") ≈ 3.0
	@test Redis.zcount(client,  "ZKEY", 3, 4) == 1
	@test Redis.zcount(client,  "ZKEY", 1, 4) == 3
	@test Redis.zrank(client,  "ZKEY", "ZVAL3") == 2
	@test Redis.zcard(client,  "ZKEY") == 3
	@test Redis.zadd(client,   "ZKEY", "XX", "CH", 3.1, "ZVAL3") == 1
	@test Redis.zadd(client,   "ZKEY", "NX", "CH", 3.1, "ZVAL3") == 0
	@test Redis.zscore(client, "ZKEY", "ZVAL3") ≈ 3.1
	@test Redis.zrank(client,  "ZKEY", "ZVAL3") == 2
	@test Redis.zcount(client,  "ZKEY", 3, 3.2) == 1
	@test Redis.zcard(client,  "ZKEY") == 3
	@test Redis.zadd(client,   "ZKEY", "XX", "CH", 3.0, "ZVAL4") == 0
	@test Redis.zadd(client,   "ZKEY", "NX", "CH", 3.0, "ZVAL4") == 1
	@test Redis.zscore(client, "ZKEY", "ZVAL4") ≈ 3.0
	@test Redis.zrank(client,  "ZKEY", "ZVAL4") == 2
	@test Redis.zadd(client,   "ZKEY", "XX", "CH", 3.1, "ZVAL4") == 1
	@test Redis.zadd(client,   "ZKEY", "NX", "CH", 3.1, "ZVAL4") == 0
	@test Redis.zscore(client, "ZKEY", "ZVAL4") ≈ 3.1
	@test Redis.zrank(client,  "ZKEY", "ZVAL4") == 3
	@test Redis.zcard(client,  "ZKEY") == 4
end

function test_multiple_add_methods(client)
	@test Redis.zcard(client, "ZKEY") == 0
	@test Redis.zadd(client, "ZKEY", [1.0, "ZVAL"]) == 1
	@test Redis.zscore(client, "ZKEY", "ZVAL") ≈ 1.0
	@test Redis.zrank(client,  "ZKEY", "ZVAL") == 0
	@test Redis.zcard(client, "ZKEY") == 1
	@test Redis.zadd(client, "ZKEY", 4.0, "ZVAL5", 4.1, "ZVAL6") == 2
	@test Redis.zscore(client, "ZKEY", "ZVAL5") ≈ 4.0
	@test Redis.zrank(client,  "ZKEY", "ZVAL5") == 1
	@test Redis.zscore(client, "ZKEY", "ZVAL6") ≈ 4.1
	@test Redis.zcard(client, "ZKEY") == 3
	@test Redis.zadd(client, "ZKEY", "CH", 4.0, "ZVAL5", 4.1, "ZVAL6") == 0
	@test Redis.zcard(client, "ZKEY") == 3
	@test Redis.zadd(client, "ZKEY", 4.2, "ZVAL5", 4.1, "ZVAL7") == 1
	@test Redis.zrank(client,  "ZKEY", "ZVAL5") == 3
	@test Redis.zrank(client,  "ZKEY", "ZVAL6") == 1
	@test Redis.zrank(client,  "ZKEY", "ZVAL7") == 2
	@test Redis.zcard(client, "ZKEY") == 4
	@test Redis.zscore(client, "ZKEY", "ZVAL5") ≈ 4.2
	@test Redis.zadd(client, "ZKEY", "CH", 4.3, "ZVAL5", 4.1, "ZVAL8") == 2
	@test Redis.zcard(client, "ZKEY") == 5
	@test Redis.zadd(client, "ZKEY", "INCR", 0.2, "ZVAL5") == 4.5
	@test Redis.zcard(client, "ZKEY") == 5
	@test Redis.zscore(client, "ZKEY", "ZVAL5") ≈ 4.5
	@test Redis.zrank(client,  "ZKEY", "ZVAL5") == 4
	@test Redis.zscore(client, "ZKEY", "ZVAL6") ≈ 4.1
	@test Redis.zrank(client,  "ZKEY", "ZVAL6") == 1
	@test Redis.zscore(client, "ZKEY", "ZVAL7") ≈ 4.1
	@test Redis.zrank(client,  "ZKEY", "ZVAL7") == 2
	@test Redis.zscore(client, "ZKEY", "ZVAL8") ≈ 4.1
	@test Redis.zrank(client,  "ZKEY", "ZVAL8") == 3
end

test_clean_client_with(test_methods)
test_clean_client_with(test_multiple_add_methods)
