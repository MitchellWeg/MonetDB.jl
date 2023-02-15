# MonetDB.jl

A pure Julia MonetDB connector.

## Usage

```julia
MonetDB.connect("localhost", 50000, "monetdb", "monetdb", "demo")

df = MonetDB.execute(conn, "SELECT 1 AS \"foo\",2 AS \"bar\"")

1×2 DataFrame
 Row │ foo     bar
     │ String  String
─────┼────────────────
   1 │ 1       2
```

