
host_var = "HOST_ENV"

if host_var in keys(ENV)
    host = ENV[host_var]
else
    host = "127.0.0.1"
end


println("===")
println("hostname: $host")
println("===")