include("Mapi.jl")
include("Monetizer.jl")

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

    if !startswith(resp_type, "&1")
        # Not a data response,
        # just return an empty DataFrame
        return df
    end
    
    split_on_newline = split(resp[1], '\n')
    column_names_raw = split(strip(split_on_newline[3][2:end-6]), '\t')

    column_names = map(strip_off_last_comma, column_names_raw)

    data_raw = split_on_newline[6:end-1]
    data_raw = map(x -> x[2:end-1], data_raw)
    data_raw = map(x -> split(x[1:end-1], '\t'), data_raw)
    data = map(x -> map(s -> strip_off_last_comma(strip(s)), x), data_raw)
    data = reduce(vcat, data)

    mdata = permutedims(reshape(data, length(column_names), :))
    df = DataFrame(mdata, column_names)

    return df
end

function strip_off_last_comma(line)
    if line[end] == ','
        line = line[1:end-1]
    end

    return string(line)
end