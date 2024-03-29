using Sockets
using SHA

struct MapiConnectionParams
    hostname::String
    username::String
    password::String
    database::String
    port::Int
end

struct MapiConnection
    params::MapiConnectionParams
    socket::TCPSocket
end

"""
Main MAPI connect protocol.
Establishes a connection, and parses the challenge.
"""
function mapi_connect(params)
    socket = Sockets.connect(params.hostname, params.port)
    conn = MapiConnection(params, socket)

    authenticate(conn, params)

    return conn
end

function authenticate(conn, params)
    challenge = get_block(conn)[1]

    parsed_challenge = parse_server_challenge(challenge, params)

    put_block(conn, parsed_challenge)

    request_for_authentication = get_block(conn)[1]

    if startswith(request_for_authentication, "!")
        throw(error(request_for_authentication))
    end

    # Check if we get redirected
    if startswith(request_for_authentication, "^mapi:merovingian:")
        authenticate(conn, params)
    end

end

"""
Parses the challenge, and generates a response.
"""
function parse_server_challenge(challenge, params)
    splitted = split(challenge, ":")     
    salt = splitted[1]
    hashed_password = bytes2hex(sha512(params.password)) * salt
    hash = bytes2hex(sha1(hashed_password))

    return "LIT:$(params.username):{SHA1}$(hash):sql:$(params.database):\n"
end

function put_block(conn, msg)
    is_last = UInt8(1)
    byte_count = length(msg)

    flag = UInt16((byte_count << 1) + is_last)

    write(conn.socket, flag);
    write(conn.socket, msg);
end


"""
Gets the block, and transforms it into a coherent String.
"""
function get_block(conn)
    data = []

    is_last = false

    while !is_last
        header = reinterpret(UInt16, read(conn.socket, 2))[1]
        byte_count = header >> 1
        is_last = Bool(header & 1)

        raw_data = get_bytes(conn, byte_count)
        push!(data, raw_data)
    end

    return data
end


function get_bytes(conn, limit)::String
    raw_data = read(conn.socket, limit)

    return String(raw_data)
end
