function monetize(data, types)
    new_data = []
    for payload in data
        for (i, val) in enumerate(payload)
            new_val = determine_type(val, types[i])
            push!(new_data, new_val)
        end
    end

    return new_data
end

function determine_type(data, type)
    if type == "decimal"
        return parse(Float64, data)
    end
    if type == "clob"
        return string(data)
    end
    if type in ["int", "tinyint"]
        return parse(Int64, data)
    end

    throw("Unknown type $type for value $data")
    return ""
end