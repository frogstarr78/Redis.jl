using Redis
using Base.Test

function only_julia_test_files(path)
	name, ext = splitext(path)
	return ext == ".jl" && split(name, '_')[end] == "test"
end

# These should match the files referenced in the redis.conf file
[rm(file) for file = filter(isfile, ["/tmp/julia-test.pid", "/tmp/redis.log", "/tmp/julia-test.rdb"])]

pwd_files = readdir(pwd())

println("Running tests:")

for test_file in filter(only_julia_test_files, pwd_files)
	test_name, ext = splitext(test_file)
	@printf " %s\n" join(split(test_name, '_')[1:end-1], '_')
	include(test_file)
end
