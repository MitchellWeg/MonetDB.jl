include("Mapi.jl")

struct MonetDBPreparedStatement
    id::Int
    query::String
end

"""
Prepare command.
Returns a prepared statement that can be used over and over.
"""
function prepare(conn, cmd)
    q = "PREPARE $cmd"

    r = mapi_execute(conn, q)

    print(r)
end
