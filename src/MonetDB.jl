module MonetDB
include("Mapi.jl")

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

end

end # module MonetDB