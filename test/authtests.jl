using Test
using MonetDB

include("constants.jl")

Test.@testset "authentication" begin
   Test.@test typeof(MonetDB.connect(host, 50000, "monetdb", "monetdb", "demo")) == MonetDB.MapiConnection
   Test.@test_throws "!InvalidCredentialsException" MonetDB.connect(host, 50000, "monetdba", "monetdb", "demo") 
   Test.@test_throws "!InvalidCredentialsException" MonetDB.connect(host, 50000, "monetdb", "monetdba", "demo") 
end