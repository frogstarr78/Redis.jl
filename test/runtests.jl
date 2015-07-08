using Redis
using Base.Test

# write your own tests here
@test 1 == 1
@test redis_type("") == "\$0\r\n\r\n"
@test redis_type("abc") == "\$3\r\nabc\r\n"
@test redis_type(None) == "\$-1\r\n"
#print("                                                         ")
#show(redis_type(["cmd", "key", "value"]))
#println("")
@test redis_type(["cmd", "key", "value"]) == "*3\r\n\$3\r\ncmd\r\n\$3\r\nkey\r\n\$5\r\nvalue\r\n"
