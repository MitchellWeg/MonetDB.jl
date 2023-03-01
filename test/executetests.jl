using Test
using MonetDB
using DataFrames

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

   Test.@testset "load with missing values" begin
      _b = [1,2,3,missing,5]
      target_df = DataFrame(a = 1:5, b = _b, c = ["1", "2", "3", "4", "5"], d = [1.0, 2.0, 3.0, missing, missing])

      conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

      MonetDB.load(conn, target_df, "missing_values_test")

      df = MonetDB.execute(conn, "SELECT * FROM missing_values_test")

      MonetDB.execute(conn, "DROP TABLE missing_values_test")
   end

   Test.@testset "prepare with args" begin
      conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

      table_name = "test_prepare_args"
      MonetDB.execute(conn, "CREATE TABLE $table_name(id INT, foo STRING, bar STRING)")
      MonetDB.execute(conn, "INSERT INTO $table_name VALUES (1, 'foo', 'bar')")
      MonetDB.execute(conn, "INSERT INTO $table_name VALUES (2, 'hello', 'there')")
      MonetDB.execute(conn, "INSERT INTO $table_name VALUES (3, 'good', 'bye')")
      MonetDB.execute(conn, "INSERT INTO $table_name VALUES (4, 'bye', 'there')")
      MonetDB.execute(conn, "INSERT INTO $table_name VALUES (5, 'over', 'there')")

      prep = MonetDB.prepare(conn, "SELECT id, foo, bar FROM $table_name WHERE bar = ?")

      df = MonetDB.execute(conn, prep, ["there"])

      prep2 = MonetDB.prepare(conn, "SELECT id, foo, bar FROM $table_name WHERE id = ?")

      df2 = MonetDB.execute(conn, prep2, [1])

      prep3 = MonetDB.prepare(conn, "SELECT id, foo, bar FROM $table_name WHERE foo = ? AND bar = ?")

      df3 = MonetDB.execute(conn, prep3, ["hello", "there"])

      Test.@test nrow(df) == 3
      Test.@test nrow(df2) == 1
      Test.@test nrow(df3) == 1

      MonetDB.deallocate(conn, prep)
      MonetDB.deallocate(conn, prep2)
      MonetDB.deallocate(conn, prep3)

      MonetDB.execute(conn, "DROP TABLE $table_name")
   end

   Test.@testset "annoying chars" begin
      target_df = DataFrame(a = ["I have a tick'"])

      conn = MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

      MonetDB.load(conn, target_df, "annoying_chars")

      df = MonetDB.execute(conn, "SELECT * FROM annoying_chars")

      MonetDB.execute(conn, "DROP TABLE annoying_chars")
   end

end