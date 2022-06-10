# To look up information for error code 1000
nats errors lookup 1000

# To list all errors mentioning stream using regular expression matches
nats errors ls stream

# As a NATS Server developer edit an existing code in errors.json
nats errors edit errors.json 10013

# As a NATS Server developer add a new code to the errors.json, auto picking a code
nats errors add errors.json
