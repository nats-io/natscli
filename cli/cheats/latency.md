# To test latency between 2 servers
nats latency --server srv1.example.net:4222 --server-b srv2.example.net:4222 --duration 10s
