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

#
# Main MAPI connect protocol.
# Establishes a connection, and parses the challenge.
#
function mapi_connect(params)
    socket = Sockets.connect(params.hostname, params.port)

    conn = MapiConnection(params, socket)
    challenge = get_block(conn)

    parsed_challenge = parse_server_challenge(challenge, params)

    put_block(conn, parsed_challenge)

    request_for_authentication = get_block(conn)

    print(request_for_authentication)
    if startswith(request_for_authentication, "^mapi:merovingian:")
        new_challenge = get_block(conn)
        new_parsed_challenge = parse_server_challenge(new_challenge, params)
        put_block(conn, new_parsed_challenge)
    end

    authenticated = get_block(conn)

    print(authenticated)

    return conn
end

#
# Parses the challenge, and generates a response.
#
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
TODO: Do something if this is not the last block.
"""
function get_block(conn)
    raw_data = readavailable(conn.socket)

    data  = String(raw_data[3:end])

    return data
end
