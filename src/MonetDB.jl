module MonetDB
include("Mapi.jl")
include("Load.jl")
include("Prepare.jl")
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
Executes a prepared statement.
"""
function execute(conn, prepared_statement::MonetDBPreparedStatement, args)
    return mapi_execute(conn, prepared_statement, args)
end


"""
Load a DataFrame into a table.
For this, a new table will be created.
"""
function load(conn, df::DataFrame, table_name)
    column_names = names(df)
    column_types = eltype.(eachcol(df))
    create_table_q = determine_create_table_query(table_name, column_names, column_types)

    transaction(conn) do 
        MonetDB.execute(conn, create_table_q)
        for row in eachrow(df)
            q = determine_execute_query(table_name, row)
            MonetDB.execute(conn, q)
        end
    end
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
