using Test
using MonetDB
using DataFrames

Test.@testset "authentication" begin
   Test.@test typeof(MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")) == MonetDB.MapiConnection
   Test.@test_throws "!InvalidCredentialsException" MonetDB.connect("localhost", 50000, "monetdba", "monetdb", "demo") 
   Test.@test_throws "!InvalidCredentialsException" MonetDB.connect("localhost", 50000, "monetdb", "monetdba", "demo") 
   Test.@test_throws "!monetdbd: no such database" MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demoa") 
end


Test.@testset "execute" begin
   conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")
   expected = DataFrame(foo=["1"], bar=["2"])
   actual = MonetDB.execute(conn, "SELECT 1 AS \"foo\",2 AS \"bar\"")

   Test.@test expected == actual
end

Test.@testset "more execute" begin
   conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

   # create_1 = MonetDB.execute(conn, "CREATE TABLE test_1(id INT, foo STRING)")
   # insert_1 = MonetDB.execute(conn, "INSERT INTO test_1 VALUES(1, \'I am foo\')")
   # insert_2 = MonetDB.execute(conn, "INSERT INTO test_1 VALUES(2, \'You are bar\')")
   # insert_3 = MonetDB.execute(conn, "INSERT INTO test_1 VALUES(3, \'We are foobar\')")
   # Test.@test size(insert_1) == (1,6)


   # delete_1 = MonetDB.execute(conn, "DROP TABLE test_1")

   # Test.@test_throws "!42S02!SELECT: no such table 'test_1'\n" MonetDB.execute(conn, "SELECT * FROM test_1")
end
