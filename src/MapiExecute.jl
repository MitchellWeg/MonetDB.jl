include("Mapi.jl")

using DataFrames

"""
Mapi Execute routine.

This routine gives the statement to the server,
and parses it result.
"""
function mapi_execute(conn, stmt)::DataFrame
    put_block(conn, "s$stmt;")

    resp = get_block(conn)

    if startswith(resp[1], "!")
        throw(error(resp[1]))
    end

    splitted = split(resp[1], ' ')
    resp_type = splitted[1]

    df = DataFrame()

    if startswith(resp_type, "&1")
        # Data Response.
    elseif startswith(resp_type, "&2")
        values = map(x -> parse(Int, strip(x)), splitted[2:end])
        names = ["Affected Rows", "Last increment Auto ID", "Query ID", "Query Time (in ms)", "MAL optimizer time", "SQL optimizer time"]
        df = create_dataframe(values, names)
    elseif startswith(resp_type, "&3")
        # Stats Only Response.
    end

    return df
end

function create_dataframe(output, column_names)
    tups = Pair[]

    for (i, name) in enumerate(column_names)
        push!(tups, (name => output[i]))
    end

    df = DataFrame(tups)

    return DataFrame(tups)
end