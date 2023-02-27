using InlineStrings

function determine_execute_query(table_name, row)
    q = "insert into $table_name values("

    for (i, col) in enumerate(row)
        if col === missing
            q = q * "NULL"
        else
            q = q * "'$col'"
        end

        if i == length(row)
            q = q * ")"
            break
        end

        q = q * ","
    end

    return q
end

function determine_create_table_query(table_name, col_names, col_types)
    @assert length(col_names) == length(col_types)

    # TODO: table_name here must be sanitized!
    suffix = "create table $table_name("

    for (i, name) in enumerate(col_names)
        latter = "$name $(get_type(col_types[i]))"
        suffix = suffix * latter

        if i == length(col_names)
            break
        end

        suffix = suffix * ", "
    end

    suffix = suffix * ")"
    return suffix
end

function get_type(type)
    
    # Column contains missing values
    if typeof(type) == Union
        type = type.b
    end


    if type == Float64 || type == Float32 || type == Float16
        return "float"
    end
    if type == Int
        return "int"
    end
    if type == String
        return "string"
    end
    if type in [InlineString1, InlineString3, InlineString7, InlineString15, InlineString31, InlineString63, InlineString127, InlineString255]
        return "string"
    end

    throw("No matching type for $type")
    return ""
end