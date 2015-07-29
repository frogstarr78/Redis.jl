using Redis
using Base.Test

include("test_client.jl")

function test_methods(client)
end

test_client_with(test_methods)
