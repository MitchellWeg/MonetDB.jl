# MonetDB.jl

A pure Julia MonetDB connector.

## Usage

### Execute

```julia
MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

df = MonetDB.execute(conn, "SELECT 1 AS \"foo\",2 AS \"bar\"")

1×2 DataFrame
 Row │ foo     bar
     │ String  String
─────┼────────────────
   1 │ 1       2
```

### Loading a DataFrame into a table

A DataFrame can be saved into a table:

```julia
MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

MonetDB.load(conn, my_df, "my_table")
```

### Prepared statements

```julia
MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

prepared_statement = MonetDB.prepare(conn, "SELECT id, foo, bar FROM my_table WHERE bar = ?")

df = MonetDB.execute(conn, prepared_statement, ["there"])
```

### Transaction

Additionally, a transaction can also be started:

```julia
MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

MonetDB.transaction(conn) do
   MonetDB.execute(conn, "INSERT INTO my_table VALUES ('foo')")
end

```