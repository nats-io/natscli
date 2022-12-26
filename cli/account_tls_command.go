// Copyright 2022 The NATS Authors
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

package cli

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/choria-io/fisk"

	"golang.org/x/crypto/ocsp"
)

type ActTLSCmd struct {
	expireWarnDuration time.Duration
	wantOCSP           bool
	wantPEM            bool

	// values after here derived inside showTLS
	now          time.Time
	warnIfBefore time.Time
}

func configureAccountTLSCommand(srv *fisk.CmdClause) {
	c := &ActTLSCmd{}

	tls := srv.Command("tls", "Report TLS chain for connected server").Action(c.showTLS)
	tls.Flag("expire-warn", "Warn about certs expiring this soon (1w; 0 to disable)").Default("1w").PlaceHolder("DURATION").DurationVar(&c.expireWarnDuration)
	tls.Flag("ocsp", "Report OCSP information, if any").UnNegatableBoolVar(&c.wantOCSP)
	tls.Flag("pem", "Show PEM Certificate blocks (true)").Default("true").BoolVar(&c.wantPEM)

	// TODO: consider NAGIOS-compatible option (output format, exit statuses)
}

func (c *ActTLSCmd) showTLS(_ *fisk.ParseContext) error {
	c.now = time.Now()
	if c.expireWarnDuration > 0 {
		c.warnIfBefore = c.now.Add(c.expireWarnDuration)
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	t, err := nc.TLSConnectionState()
	if err != nil {
		return err
	}

	var showingOCSP bool
	if c.wantOCSP {
		if t.OCSPResponse != nil && len(t.OCSPResponse) > 0 {
			showingOCSP = true
		} else {
			fmt.Printf("# No OCSP Response found in TLS connection\n\n")
		}
	}

	fmt.Printf("# TLS Verified Chains count: %d\n", len(t.VerifiedChains))
	if len(t.VerifiedChains) < 1 {
		return fmt.Errorf("no verified chains found in TLS")
	}

	err = nil
	for i := range t.VerifiedChains {
		fmt.Printf("\n# chain: %d\n", i+1)
		if chainErr := c.showOneTLSChain(t.VerifiedChains[i], i+1); chainErr != nil && err == nil {
			err = chainErr
		}
		if showingOCSP {
			if ocspErr := c.showOneOCSP(t.VerifiedChains[i], i+1, t); ocspErr != nil && err == nil {
				err = ocspErr
			}
		}
	}
	return err
}

func (c *ActTLSCmd) showOneTLSChain(chain []*x509.Certificate, chain_number int) error {
	var err error
	for ci, cert := range chain {
		fmt.Printf("# chain=%d cert=%d isCA=%v Subject=%q\n", chain_number, ci+1, cert.IsCA, cert.Subject.String())
		if cert.NotAfter.Before(c.now) {
			// I don't think this should happen because we're for verified chains, but protect against being called on an unverified chain.
			fmt.Printf("# EXPIRED after %v\n", cert.NotAfter)
			if err == nil {
				err = fmt.Errorf("expired cert chain=%d cert=%d expiration=%q subject=%q", chain_number, ci+1, cert.NotAfter, cert.Subject.String())
			}
		} else if cert.NotAfter.Before(c.warnIfBefore) {
			fmt.Printf("# EXPIRING SOON: within %v of %v\n", c.expireWarnDuration, cert.NotAfter)
			if err == nil {
				err = fmt.Errorf("cert expiring soon chain=%d cert=%d expiration=%q subject=%q", chain_number, ci+1, cert.NotAfter, cert.Subject.String())
			}
		}
		// Always show expiration in this form, even if already shown, to have a stable grep pattern
		fmt.Printf("#   Expiration: %s\n", cert.NotAfter)
		if len(cert.DNSNames) > 0 {
			fmt.Printf("#   SAN: DNS Names: %v\n", cert.DNSNames)
		}
		if len(cert.IPAddresses) > 0 {
			fmt.Printf("#   SAN: IP Addresses: %v\n", cert.IPAddresses)
		}
		if len(cert.URIs) > 0 {
			fmt.Printf("#   SAN: URIs: %v\n", cert.URIs)
		}
		if len(cert.EmailAddresses) > 0 {
			fmt.Printf("#   SAN: Email Addresses: %v\n", cert.EmailAddresses)
		}
		fmt.Printf("#   Serial: %v\n#   Signed-with: %v\n", cert.SerialNumber, cert.SignatureAlgorithm)
		if c.wantPEM {
			pem.Encode(os.Stdout, &pem.Block{
				Type:  "CERTIFICATE",
				Bytes: cert.Raw,
			})
		}
	}
	return err
}

func (c *ActTLSCmd) showOneOCSP(chain []*x509.Certificate, chain_number int, cs tls.ConnectionState) error {
	if len(chain) < 2 {
		fmt.Printf("\n# Skipping OCSP verification for solo end-entity cert chain\n")
		return nil
	}

	liveStaple, err := ocsp.ParseResponseForCert(cs.OCSPResponse, chain[0], chain[1])
	if err != nil {
		errContext := fmt.Sprintf("OCSP response invalid for chain %d's %q from %q", chain_number, chain[0].Subject, chain[1].Subject)
		fmt.Printf("\n# %s: %v\n", errContext, err)
		return fmt.Errorf("%s: %w", errContext, err)
	}

	switch liveStaple.Status {
	case ocsp.Good:
		fmt.Printf("\n# OCSP: GOOD status=%v sn=%v producedAt=(%s) thisUpdate=(%s) nextUpdate=(%s)",
			liveStaple.Status, liveStaple.SerialNumber,
			liveStaple.ProducedAt, liveStaple.ThisUpdate, liveStaple.NextUpdate)
	case ocsp.Revoked:
		fmt.Printf("\n# OCSP: REVOKED status=%v RevokedAt=(%s)", liveStaple.Status, liveStaple.RevokedAt)
	default:
		fmt.Printf("\n# OCSP: BAD status=%v sn=%v", liveStaple.Status, liveStaple.SerialNumber)
	}

	// should we return an error for OCSP bad/revoked status?
	return nil
}
