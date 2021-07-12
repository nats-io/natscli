// Copyright 2020 The NATS Authors
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

package main

import (
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

func enableTLSDebug(no *nats.Options) error {
	if no.TLSConfig == nil {
		no.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	priorCB := no.TLSConfig.VerifyConnection
	no.TLSConfig.VerifyConnection = func(cs tls.ConnectionState) error {
		if priorCB != nil {
			if err := priorCB(cs); err != nil {
				return err
			}
		}
		if cs.PeerCertificates == nil {
			log.Printf("tls-dump-certs: no certs presented by server")
			return nil
		}
		if cs.VerifiedChains == nil || len(cs.VerifiedChains) == 0 {
			// This branch should only be possible is InsecureSkipVerify was set or someone has overridden the normal verification logic.
			log.Printf("tls-dump-certs: no verified chains for certs presented by server (is verification disabled?)")
			return nil
		}
		log.Printf("tls-dump-certs: verified chains of certificates: %d", len(cs.VerifiedChains))

		// There's about 4 or so chains for a CA such as Let's Encrypt, so
		// showing all certs in PEM form for all the chains is a little too
		// voluminous.
		//
		// Note that chains are some improper subset of the certs as presented
		// by the server, possibly augmented by certificates from the
		// configured trust store.  So showing the cert in PEM form from the
		// VerifiedChains would be misleading: to me, seeing a PEM block in the
		// output suggests it was sent by the server.
		//
		// To balance these out, we're going to log details of all the chains,
		// then show just the certs as presented by the server.

		for chain := 0; chain < len(cs.VerifiedChains); chain++ {
			certCount := len(cs.VerifiedChains[chain])
			log.Printf("tls-dump-certs: chain %d: certificates: %d", chain+1, certCount)
			for certI := 0; certI < certCount; certI++ {
				log.Printf("tls-dump-certs: chain %d: certificate: %d subject: %q",
					chain+1, certI+1,
					cs.VerifiedChains[chain][certI].Subject.String())
			}
		}

		// Assume no more than 100 lines for one cert, should be overkill.
		certCount := len(cs.PeerCertificates)
		log.Printf("tls-dump-certs: server-presented certificate count: %d", certCount)
		pemData := make([]byte, 0, 100*70*certCount)
		for certI := 0; certI < certCount; certI++ {
			pemData = append(pemData, []byte(fmt.Sprintf("# [%d/%d] subject=%q expiration=%q\n",
				certI+1, certCount,
				cs.PeerCertificates[certI].Subject.String(),
				cs.PeerCertificates[certI].NotAfter,
			))...)
			dnsNames := cs.PeerCertificates[certI].DNSNames
			for i := 0; i < len(dnsNames); i++ {
				pemData = append(pemData, []byte(fmt.Sprintf("# DNS name: %q\n", dnsNames[i]))...)
			}
			pemData = append(pemData, pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: cs.PeerCertificates[certI].Raw,
			})...)
		}
		fmt.Print(string(pemData))

		return nil
	}
	return nil
}

var _ nats.Option = enableTLSDebug
