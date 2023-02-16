module MonetDB
include("Mapi.jl")
include("MapiExecute.jl")

"""
Connects to a MonetDB instance.
"""
function connect(host, port, username, password, database)
    params = MapiConnectionParams(host, username, password, database, port)
    mapi_connect(params)
end

"""
Executes a statement.
"""
function execute(conn, cmd)
    return mapi_execute(conn, cmd)
end

"""
Use a transaction to complete the statement.
"""
function transaction(f::Function, conn)
    mapi_execute(conn, "start transaction")

    try
        f()
    catch
        mapi_execute(conn, "rollback")
        return
    end

    mapi_execute(conn, "commit")
end

end # module MonetDB
