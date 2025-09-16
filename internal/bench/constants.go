package bench

const (
	DefaultDurableConsumerName = "nats-bench"
	DefaultStreamName          = "benchstream"
	DefaultBucketName          = "benchbucket"
	DefaultServiceName         = "nats-bench-service"
	DefaultServiceVersion      = "1.0.0"
	TypeCorePub                = "pub"
	TypeCoreSub                = "sub"
	TypeServiceRequest         = "request"
	TypeServiceServe           = "reply"
	TypeJSPubSync              = "jsyncpub"
	TypeJSPubAsync             = "jsasyncpub"
	TypeJSPubBatch             = "jsbatchpub"
	TypeJSOrdered              = "jsordered"
	TypeJSConsume              = "jsconsume"
	TypeJSFetch                = "jsfetch"
	TypeJSGetSync              = "jsgetdirectsync"
	TypeJSGetDirectBatched     = "jsgetdirectbatch"
	TypeOldJSOrdered           = "oldjsordered"
	TypeOldJSPush              = "oldjspush"
	TypeOldJSPull              = "oldjspull"
	TypeKVPut                  = "kvput"
	TypeKVGet                  = "kvget"
	AckModeNone                = "none"
	AckModeAll                 = "all"
	AckModeExplicit            = "explicit"
)

func GetBenchTypeLabel(benchType string) string {
	switch benchType {
	case TypeCorePub:
		return "Core NATS publisher"
	case TypeCoreSub:
		return "Core NATS subscriber"
	case TypeServiceRequest:
		return "Core NATS service requester"
	case TypeServiceServe:
		return "Core NATS service server"
	case TypeJSPubSync:
		return "JetStream synchronous publisher"
	case TypeJSPubAsync:
		return "JetStream asynchronous publisher"
	case TypeJSPubBatch:
		return "JetStream batched publisher"
	case TypeJSOrdered:
		return "JetStream ordered ephemeral consumer"
	case TypeJSConsume:
		return "JetStream durable consumer (callback)"
	case TypeJSFetch:
		return "JetStream durable consumer (fetch)"
	case TypeJSGetSync:
		return "JetStream synchronous getter"
	case TypeJSGetDirectBatched:
		return "JetStream batched direct getter"
	case TypeKVPut:
		return "JetStream KV putter"
	case TypeKVGet:
		return "JetStream KV getter"
	case TypeOldJSOrdered:
		return "JetStream ordered ephemeral consumer (old API)"
	case TypeOldJSPush:
		return "JetStream durable push consumer (old API)"
	case TypeOldJSPull:
		return "JetStream durable pull consumer (old API)"
	default:
		return "Unknown benchmark"
	}
}
