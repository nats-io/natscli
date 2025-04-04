// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package options

import (
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var DefaultOptions *Options

// Options configure the CLI
type Options struct {
	// Config is a nats configuration context
	Config *natscontext.Context
	// Servers is the list of servers to connect to
	Servers string
	// Creds is nats credentials to authenticate with
	Creds string
	// TlsCert is the TLS Public Certificate
	TlsCert string
	// TlsKey is the TLS Private Key
	TlsKey string
	// TlsCA is the certificate authority to verify the connection with
	TlsCA string
	// Timeout is how long to wait for operations
	Timeout time.Duration
	// ConnectionName is the name to use for the underlying NATS connection
	ConnectionName string
	// Username is the username or token to connect with
	Username string
	// Password is the password to connect with
	Password string
	// Token is the token to connect with
	Token string
	// Nkey is the file holding a nkey to connect with
	Nkey string
	// JsApiPrefix is the JetStream API prefix
	JsApiPrefix string
	// JsEventPrefix is the JetStream events prefix
	JsEventPrefix string
	// JsDomain is the domain to connect to
	JsDomain string
	// CfgCtx is the context name to use
	CfgCtx string
	// Trace enables verbose debug logging
	Trace bool
	// Customer inbox Prefix
	InboxPrefix string
	// Conn sets a prepared connect to connect with
	Conn *nats.Conn
	// Mgr sets a prepared jsm Manager to use for JetStream access
	Mgr *jsm.Manager
	// JSc is a prepared NATS JetStream context to use for KV and Object access
	JSc jetstream.JetStream
	// Disables registering of CLI cheats
	NoCheats bool
	// PrometheusNamespace is the namespace to use for prometheus format output in server check
	PrometheusNamespace string
	// SocksProxy is a SOCKS5 proxy to use for NATS connections
	SocksProxy string
	// ColorScheme influence table colors and more based on ValidStyles()
	ColorScheme string
	// TlsFirst configures the TLSHandshakeFirst behavior in nats.go
	TlsFirst bool
	// WinCertStoreType enables windows cert store - user or machine
	WinCertStoreType string
	// WinCertStoreMatchBy configures how to search for certs when using match - subject or issuer
	WinCertStoreMatchBy string
	// WinCertStoreMatch is the query to match with
	WinCertStoreMatch string
	// WinCertCaStoreMatch is the queries for CAs to use
	WinCertCaStoreMatch []string
}
