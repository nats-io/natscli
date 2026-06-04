# Basic steps for setting up decentralized authentication
All configuration changes are stored locally until `nats auth account push`ed to a nats cluster.
    
# Create a new operator and set as working context
nats auth operator add sysopp

# Generate a template server configuration file from an operator
nats server generate server.conf

# Create a new account
nats auth account add MyAccount

# Create a new user in an account
nats auth user add MyUser

# Create an admin user in system account
nats auth user add admin SYSTEM

# Export credentials for a user
nats auth user credential sys_admin.cred admin SYSTEM

# Push an account or its changes from a specific operator to a specific server, using system account credentials. 
nats auth account push MyAccount --server nats://localhost:4222 --operator sysopp --creds sys_admin.cred  

# Use `nats context` and `nats auth operator select` to set defaults
nats context add sysadmin --description "System Account" --server nats://localhost:4222 --creds sys_admin.cred

nats auth operator select sysopp

# Push account with default settings
nats auth account push MyAccount


