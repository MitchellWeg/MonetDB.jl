using Test
using MonetDB

Test.@testset "authentication" begin
   Test.@test typeof(MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")) == MonetDB.MapiConnection
   Test.@test_throws "!InvalidCredentialsException" MonetDB.connect("localhost", 50000, "monetdba", "monetdb", "demo") 
   Test.@test_throws "!InvalidCredentialsException" MonetDB.connect("localhost", 50000, "monetdb", "monetdba", "demo") 
   Test.@test_throws "!monetdbd: no such database" MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demoa") 
end


Test.@testset "execute" begin
   conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")
end