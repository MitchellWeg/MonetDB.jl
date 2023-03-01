using MonetDB

Test.@testset "determine query's" begin
   # TODO: This testset needs a lot more work.
   # the column names needs to be more sanitized.

   Test.@testset "simple example" begin
      q = MonetDB.determine_create_table_query("foo", ["bar"], [Int])

      Test.@test q == "create table \"foo\"(\"bar\" int)"
   end

   Test.@testset "example with SQL keyword" begin
      q = MonetDB.determine_create_table_query("foo", ["time", "position"], [Int, Int])

      Test.@test q == "create table \"foo\"(\"time\" int, \"position\" int)"
   end

end