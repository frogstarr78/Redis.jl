using Redis
using Base.Test

include("test_client.jl")

function test_methods(client)
	@test Redis.hexists(client, "KEY", "HKEY") == 0
	@test Redis.hset(client, "KEY", "HKEY", "HVAL") == true
	@test Redis.hset(client, "KEY", "HKEY2", "HVAL2") == true
	@test Redis.hlen(client, "KEY") == 2
	@test Redis.hexists(client, "KEY", "HKEY") == 1
	@test Redis.hexists(client, "KEY", "HKEY2") == 1
	@test sort(Redis.hkeys(client, "KEY")) == sort(["HKEY", "HKEY2"])
	@test Redis.hkeys(client, "KEY", sorted=true) == ["HKEY", "HKEY2"]
	@test Redis.hget(client, "KEY", "HKEY") == "HVAL"
	@test Redis.hget(client, "KEY", "HKEY") == "HVAL"
	@test Redis.hstrlen(client, "KEY", "HKEY") == 4
	@test Redis.hvals(client, "KEY") == sort(["HVAL", "HVAL2"])
	@test Redis.hvals(client, "KEY", sorted=true) == ["HVAL", "HVAL2"]
	@test Redis.hgetall(client, "KEY") == ["HKEY" => "HVAL", "HKEY2" => "HVAL2"]

	@test Redis.hset(client, "KEYI", "HKEY", 0) == true
	@test Redis.hincr(client, "KEYI", "HKEY") == 1
	@test Redis.hincrby(client, "KEYI", "HKEY", 4) == 5
	@test Redis.hincrby(client, "KEYI", "HKEY", 4.1) == 9.1
	@test Redis.hincrbyfloat(client, "KEYI", "HKEY", 2.1) == 11.2
	@test Redis.hdecrbyfloat(client, "KEYI", "HKEY", 1.2) == 10
	@test Redis.hdecr(client, "KEYI", "HKEY") == 9
	@test Redis.hdecrby(client, "KEYI", "HKEY", 4) == 5
	@test Redis.hdecrby(client, "KEYI", "HKEY", 1.2) == 3.8
end

function test_multiple_methods(client)
	Redis.flushdb(client)
	@test Redis.hmset(client, "KEY", [["HKEY3" "HKEY4"], ["HVAL3" "HVAL4"]]) == "OK"
	@test Redis.hgetall(client, "KEY") == ["HKEY3" => "HVAL3", "HKEY4" => "HVAL4"]
	@test Redis.hmset(client, "KEY", ["HKEY5" "HVAL5"]) == "OK"
	@test Redis.hgetall(client, "KEY") == ["HKEY3" => "HVAL3", "HKEY4" => "HVAL4", "HKEY5" => "HVAL5"]
	@test Redis.hmset(client, "KEY", ["HKEY6", "HVAL6"]) == "OK"
	@test Redis.hgetall(client, "KEY") == ["HKEY3" => "HVAL3", "HKEY4" => "HVAL4", "HKEY5" => "HVAL5", "HKEY6" => "HVAL6"]
	@test Redis.hmset(client, "KEY", "HKEY7", "HVAL7") == "OK"
	@test Redis.hgetall(client, "KEY") == ["HKEY3" => "HVAL3", "HKEY4" => "HVAL4", "HKEY5" => "HVAL5", "HKEY6" => "HVAL6", "HKEY7" => "HVAL7"]
	@test Redis.hlen(client, "KEY") == 5

	@test Redis.hmget(client, "KEY", "HKEY5", "HKEY6") == ["HVAL5", "HVAL6"]
	@test Redis.hmget(client, "KEY", ["HKEY5", "HKEY7"]) == ["HVAL5", "HVAL7"]
	@test Redis.hget(client, "KEY", ["HKEY5", "HKEY4"]) == ["HVAL5", "HVAL4"]
end

function test_scan_method(client)
	@test "hscan" == "unimplemented test"
end

test_client_with(test_methods)
test_client_with(test_multiple_methods)
test_client_with(test_scan_method)
