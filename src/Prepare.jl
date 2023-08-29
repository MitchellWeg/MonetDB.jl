
struct MonetDBPreparedStatement
    id::Int
    query::String
end

"""
Prepare command.
Returns a prepared statement that can be used over and over.
"""
function prepare(conn, cmd)::MonetDBPreparedStatement
    q = "PREPARE $cmd"

    r = mapi_execute(conn, q)

    id = parse(Int, r[1, 1])
    prep = MonetDBPreparedStatement(id, q)

    return prep
end

"""
Deallocates a prepared statement
"""
function deallocate(conn, prep::MonetDBPreparedStatement)
    q = "DEALLOCATE $(prep.id)"
    mapi_execute(conn, q)
end
