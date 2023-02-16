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

    if !startswith(resp_type, "&1")
        # Not a data response,
        # just return an empty DataFrame
        return parse_non_data_response(resp)
    end
    
    df = parse_data_response(resp)

    return df
end

"""
We got a data response, denoted by the header, which indicated by setting a '&1' in the first couple of bytes.
This function parses the response got from the server,
and returns a dataframe with the data.
"""
function parse_data_response(resp)::DataFrame
    split_on_newline = split(resp[1], '\n')
    column_names_raw = split(strip(split_on_newline[3][2:end-6]), '\t')

    column_names = map(strip_off_last_comma, column_names_raw)

    col_types = split(strip(split_on_newline[4][2:end-6]), '\t')
    col_types = map(strip_off_last_comma, col_types)

    data_raw = split_on_newline[6:end-1]
    data_raw = map(x -> x[2:end-1], data_raw)
    data_raw = map(x -> split(x[1:end-1], '\t'), data_raw)
    data = map(x -> map(s -> strip_off_last_comma(strip(s)), x), data_raw)
    data = monetize(data, col_types)
    data = reduce(vcat, data)

    mdata = permutedims(reshape(data, length(column_names), :))
    df = DataFrame(mdata, column_names)

    return df
end

"""
We got a non data response, denoted by the header, which didn't set it as '&1'.
This function creates a DataFrame from the metadata retrieved from the server.
"""
function parse_non_data_response(resp)::DataFrame
    df = DataFrame()

    return df
end

function strip_off_last_comma(line)
    if line[end] == ','
        line = line[1:end-1]
    end

    return string(line)
end