# To set up a responder that runs an external command with the 3rd subject token as argument
nats reply "service.requests.>" --command "service.sh {{2}}"

# To set up basic responder
nats reply service.requests "Message {{Count}} @ {{Time}}"
nats reply service.requests --echo --sleep 10
