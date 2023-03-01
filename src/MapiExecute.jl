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
    
    split_on_newline = split(resp[1], '\n')
    header = split(split_on_newline[1], ' ')
    query_id = parse(Int, header[2])
    row_count = parse(Int, header[3])

    column_names_raw = split(strip(split_on_newline[3][2:end-6]), '\t')

    column_names = map(strip_off_last_comma, column_names_raw)

    col_types = split(strip(split_on_newline[4][2:end-6]), '\t')
    col_types = map(strip_off_last_comma, col_types)

    data_raw = split_on_newline[6:end-1]
    current_data_length = length(data_raw)

    # We didn't receive everything.
    # This is because of the Xreply size.
    # Paginate to ask for the rest.
    if current_data_length < row_count
        remaining_data = paginate(conn, query_id, current_data_length, row_count)  
        split_remain = split(remaining_data[1], '\n')
        data_raw = vcat(data_raw, split_remain[2:end-1])
    end

    df = parse_data_response(col_types, column_names, data_raw)

    return df
end

"""
Mapi execute routine with a prepared statement
"""
function mapi_execute(conn, prep::MonetDBPreparedStatement, args)::DataFrame
    @assert length(args) > 0

    df = DataFrame()
    _args = []

    for arg in args
       s = "'$arg'" 
       push!(_args, s)
    end

    v = join(_args, ',')

    stmt = "EXECUTE $(prep.id)($v)"

    return mapi_execute(conn, stmt)
end
"""
We got a data response, denoted by the header, which indicated by setting a '&1' in the first couple of bytes.
This function parses the response got from the server,
and returns a dataframe with the data.
"""
function parse_data_response(column_types, column_names, data_raw)::DataFrame
    data_raw = map(x -> x[2:end-1], data_raw)
    data_raw = map(x -> split(x[1:end-1], '\t'), data_raw)
    data = map(x -> map(s -> strip_off_last_comma(strip(s)), x), data_raw)
    data = monetize(data, column_types)
    data = reduce(vcat, data)

    mdata = permutedims(reshape(data, length(column_names), :))
    df = DataFrame(mdata, column_names)

    return df
end

"""
Xreply_size by default is set to 100. This means it will only reply with 100 rows.
When the result set is bigger, we need to paginate to get the rest.
"""
function paginate(conn, query_id, current_row_id, max)
    # TODO: enable chunking.
    # Now it just returns all of the remaining data.
    # We should be able to request everything in chunks.
    put_block(conn, "Xexport $query_id $current_row_id $(max + 1)")

    return get_block(conn) 
end

"""
We got a non data response, denoted by the header, which didn't set it as '&1'.
This function creates a DataFrame from the metadata retrieved from the server.
"""
function parse_non_data_response(resp)::DataFrame
    header = split(resp[1], '\n')[1]
    header_split = split(header, ' ')

    c = collect(header_split[2:end])

    df = DataFrame(metadata=c)

    return df
end

function strip_off_last_comma(line)
    if line[end] == ','
        line = line[1:end-1]
    end

    return string(line)
end