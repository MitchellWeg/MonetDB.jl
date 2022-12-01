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
    socket = Sockets.connect("127.0.0.1", params.port)
    challenge = read(socket)
    print(challenge)

    conn = MapiConnection(params, socket)

    parsed_challenge = parse_server_challenge(challenge, params)
    print(parsed_challenge)

    put_block(conn, parsed_challenge)

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

    return "SUIIIII"
    # return "LIT:$(params.username):{SHA1}$(hash):sql:$(params.database):\n"
end

function put_block(conn, msg)
    is_last = UInt8(1)
    byte_count = length(msg)

    flag = UInt16((byte_count << 1) + is_last)
    print(flag)

    payload = Vector{UInt8}(msg)

    write(conn.socket, flag);
    write(conn.socket, msg);

    resp = read(conn.socket)
    print("resp:\n")
    print(resp)

end