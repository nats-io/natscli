package cli

import (
	"fmt"
	"math"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/archive"
)

const (
	clusterMemoryUsageThresholdPercentage = 0.3
)

type PaAnalyzeCmd struct {
	archivePath string
}

type checkFunc func(r *archive.Reader) error

var checks = []checkFunc{
	checkServerVersions,
	checkSlowConsumers,
	checkClusterMemoryUsage,
}

func configurePaAnalyzeCommand(srv *fisk.CmdClause) {
	c := &PaAnalyzeCmd{}

	analyze := srv.Command("analyze", "create archive of monitoring data for all servers and accounts").Action(c.analyze)
	analyze.Arg("archivePath", "path of archive to extract information from and analyze").Required().StringVar(&c.archivePath)
}

func (c *PaAnalyzeCmd) analyze(_ *fisk.ParseContext) error {
	ar, err := archive.NewReader(c.archivePath)
	if err != nil {
		return err
	}
	defer ar.Close()

	for _, checkFunc := range checks {
		err := checkFunc(ar)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkServerVersions(r *archive.Reader) error {
	var (
		serverTags     = r.ListServerTags()
		serverVersions = make(map[string][]string, len(serverTags))
	)

	for _, serverTag := range serverTags {
		serverVarz := server.Varz{}
		r.Load(&serverVarz, &serverTag, archive.TagServerVars())
		_, exists := serverVersions[serverVarz.Version]
		if !exists {
			serverVersions[serverVarz.Version] = []string{}
		}
		serverVersions[serverVarz.Version] = append(serverVersions[serverVarz.Version], serverTag.Value)
	}

	if len(serverVersions) == 1 {
		fmt.Println("✅ All servers are running the same version")
	} else {
		fmt.Println("🔔 WARNING: Servers are running different versions")
		fmt.Println("--- SERVER VERSIONS ---")
		for version, servers := range serverVersions {
			fmt.Printf("%s: %v servers\n", version, servers)
		}
	}

	return nil
}

func checkSlowConsumers(r *archive.Reader) error {
	serverTags := r.ListServerTags()
	for _, serverTag := range serverTags {
		serverVarz := server.Varz{}
		r.Load(&serverVarz, &serverTag, archive.TagServerVars())
		if serverVarz.SlowConsumers > 0 {
			fmt.Printf("🔔 WARNING: %v slow consumers on server %v\n", serverVarz.SlowConsumers, serverTag.Value)
		}
	}
	fmt.Println("✅ No slow consumers found")
	return nil
}

func checkClusterMemoryUsage(r *archive.Reader) error {
	serverTags := r.ListServerTags()
	clusterMemoryUsage := make(map[string]float64, len(serverTags))
	for _, serverTag := range serverTags {
		serverVarz := server.Varz{}
		r.Load(&serverVarz, &serverTag, archive.TagServerVars())
		clusterMemoryUsage[serverTag.Value] = float64(serverVarz.Mem)
	}

	average := func(m map[string]float64) float64 {

		var (
			median float64
			count  int
		)

		for _, v := range m {
			median += v
			count++
		}

		return median / float64(count)
	}

	medianMem := average(clusterMemoryUsage)
	medianMemMb := medianMem / 1024 / 1024
	allGood := true
	for server, mem := range clusterMemoryUsage {
		if math.Abs(mem-medianMem)/medianMem > clusterMemoryUsageThresholdPercentage {
			allGood = false
			memMb := mem / 1024 / 1024
			fmt.Printf("🔔 WARNING: %v memory usage (%.2fMb) difference from median (%.2fMb) is over %.2f%% threshold\n", server, memMb, medianMemMb, clusterMemoryUsageThresholdPercentage)
		}
	}
	if allGood {
		fmt.Println("✅ Cluster memory usage for all servers is within threshold")
	}

	return nil
}
