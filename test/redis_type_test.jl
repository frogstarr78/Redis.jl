using Redis
using Base.Test

@test Redis.redis_type("") == string('$', "0\r\n\r\n")
@test Redis.redis_type("abc") == string('$', "3\r\nabc\r\n")
@test Redis.redis_type(None) == string('$', "-1\r\n")
@test Redis.redis_type(["cmd", "key", "value"]) == string("*3\r\n", '$', "3\r\nCMD\r\n", '$', "3\r\nkey\r\n", '$', "5\r\nvalue\r\n")
@test Redis.redis_type(["cmd"]) == string("*1\r\n", '$', "3\r\nCMD\r\n")
