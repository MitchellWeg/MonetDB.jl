host = "127.0.0.1"

host_var = "JULIA_HOST_ENV"

if host_var in keys(ENV)
    host = ENV[host_var]
end

print("===")
print("hostname: $host")
print("===")