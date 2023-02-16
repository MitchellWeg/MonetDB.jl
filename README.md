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

### Transaction

Additionally, a transaction can also be started:

```julia
MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

MonetDB.transaction(conn) do
   MonetDB.execute(conn, "INSERT INTO my_table VALUES ('foo')")
end

```