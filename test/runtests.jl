using Test
using MonetDB
using DataFrames

Test.@testset "authentication" begin
   Test.@test typeof(MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")) == MonetDB.MapiConnection
   Test.@test_throws "!InvalidCredentialsException" MonetDB.connect("localhost", 50000, "monetdba", "monetdb", "demo") 
   Test.@test_throws "!InvalidCredentialsException" MonetDB.connect("localhost", 50000, "monetdb", "monetdba", "demo") 
end


Test.@testset "execute" begin

   Test.@testset "simple example" begin
      conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")
      expected = DataFrame(foo=[1], bar=[2])
      actual = MonetDB.execute(conn, "SELECT 1 AS \"foo\",2 AS \"bar\"")
      
      Test.@test expected == actual
   end

   Test.@testset "more execute" begin
      conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

      create_1 = MonetDB.execute(conn, "create table test_1(id int, foo string)")
      insert_1 = MonetDB.execute(conn, "INSERT INTO test_1 VALUES(1, \'I am foo\')")
      insert_2 = MonetDB.execute(conn, "INSERT INTO test_1 VALUES(2, \'You are bar\')")
      insert_3 = MonetDB.execute(conn, "INSERT INTO test_1 VALUES(3, \'We are foobar\')")

      expected = DataFrame(id=[1,2,3], foo=["\"I am foo\"", "\"You are bar\"", "\"We are foobar\""])
      actual = MonetDB.execute(conn, "SELECT * FROM test_1")

      delete_1 = MonetDB.execute(conn, "DROP TABLE test_1")

      Test.@test_throws "!42S02!SELECT: no such table 'test_1'\n" MonetDB.execute(conn, "SELECT * FROM test_1")
   end


   Test.@testset "other execute types" begin
      conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")
      expected = DataFrame(foo=[1.0], bar=[2.5])
      actual = MonetDB.execute(conn, "SELECT 1.0 AS \"foo\",2.5 AS \"bar\"")

      Test.@test expected == actual
   end

   Test.@testset "transaction" begin
      conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

      MonetDB.execute(conn, "create table test_2(id int, foo string)")

      MonetDB.transaction(conn) do 
         MonetDB.execute(conn, "INSERT INTO test_2 VALUES (1, 'foo')")
         MonetDB.execute(conn, "INSERT INTO test_2 VALUES (2, 'bar')")
         MonetDB.execute(conn, "INSERT INTO test_2 VALUES (3, 'baz')")
      end

      expected = DataFrame(id=[1,2,3], foo=["\"foo\"", "\"bar\"", "\"baz\""])
      actual = MonetDB.execute(conn, "SELECT * FROM test_2")

      Test.@test expected == actual

      MonetDB.execute(conn, "DROP TABLE test_2")
   end

   Test.@testset "load" begin
      target_df = DataFrame(a = 1:150, b = rand(150))

      conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

      MonetDB.load(conn, target_df, "foo")

      df = MonetDB.execute(conn, "SELECT * FROM foo")

      Test.@test nrow(target_df) == nrow(df)
      Test.@test target_df == df

      MonetDB.execute(conn, "DROP TABLE foo")
   end
end
