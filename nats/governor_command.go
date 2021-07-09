package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"time"

	"github.com/kballard/go-shellquote"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/governor"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type govCmd struct {
	name     string
	ident    string
	limit    uint64
	replicas uint
	interval time.Duration
	age      time.Duration
	force    bool
	id       uint64
	command  string
}

func configureGovernorCommand(app *kingpin.Application) {
	c := &govCmd{}

	help := `Controls the concurrency of distributed command executions

A Governor controls the execution of commands by creating a stack
of limited size. Before a command is executed a slot on the stack
should be successfully gained. Once gained the command runs and
afterward the slot is freed.

If the command takes too long to run - or crashed - the slot will
be released after a prior.

This command allow for creation, viewing, management and execution
of Governors.

JetStream is required to use this feature.

NOTE: This is an experimental feature.
`

	gov := app.Command("governor", help).Alias("gov")

	add := gov.Command("add", "Adds a new Governor to JetStream").Action(c.addAction)
	add.Arg("name", "Governor name").Required().StringVar(&c.name)
	add.Arg("limit", "Maximum executions allowed").Required().Uint64Var(&c.limit)
	add.Arg("age", "Maximum time a entry can stay before being timed out").Required().DurationVar(&c.age)
	add.Flag("replicas", "Stream replica level").Default("1").UintVar(&c.replicas)
	add.Flag("force", "Force the create/update without prompting").BoolVar(&c.force)

	view := gov.Command("view", "Views the status of the Governor").Alias("info").Alias("v").Action(c.viewAction)
	view.Arg("name", "Governor name").Required().StringVar(&c.name)

	reset := gov.Command("reset", "Resets the Governor by removing all entries").Action(c.resetAction)
	reset.Arg("name", "Governor name").Required().StringVar(&c.name)
	reset.Flag("force", "Force reset without prompting").BoolVar(&c.force)

	evict := gov.Command("evict", "Removes a entry from the Governor").Action(c.evictAction)
	evict.Arg("name", "Governor name").Required().StringVar(&c.name)
	evict.Arg("id", "The ID to remove").Uint64Var(&c.id)
	evict.Flag("force", "Force eviction without prompting").BoolVar(&c.force)

	rm := gov.Command("rm", "Removes a Governor").Action(c.rmAction)
	rm.Arg("name", "Governor name").Required().StringVar(&c.name)
	rm.Flag("force", "Force eviction without prompting").BoolVar(&c.force)

	run := gov.Command("run", "Runs a command limited by the Governor").Action(c.runAction)
	run.Arg("name", "Governor name").Required().StringVar(&c.name)
	run.Arg("identity", "Identity to record in the Governor").Required().StringVar(&c.ident)
	run.Arg("command", "Command to execute").Required().StringVar(&c.command)
	run.Flag("max-wait", "Maximum amount of time to wait to obtain a lease").Default("5m").DurationVar(&c.age)
	run.Flag("interval", "Interval for attempting to get an execution slot").Default("2s").DurationVar(&c.interval)

	cheats["governor"] = `# to create governor with 10 slots and 1 minute timeout
nats governor add cron 10 1m

# to view the configuration and state
nats governor view cron

# to reset the governor, clearing all slots
nats governor reset cron

# to run long-job.sh when a slot is available, giving up after 20 minutes without a slot
nats governor run cron $(hostname -f) --max-wait 20m long-job.sh'
`
}

func (c *govCmd) rmAction(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	gmgr, err := governor.NewJSGovernorManager(c.name, c.limit, c.age, c.replicas, mgr, false)
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Remove the %s Governor?", c.name), false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("skipped")
		}
	}

	return gmgr.Stream().Delete()
}

func (c *govCmd) addAction(pc *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	gmgr, err := governor.NewJSGovernorManager(c.name, c.limit, c.age, c.replicas, mgr, false)
	if err != nil {
		return err
	}

	if gmgr.MaxAge() != c.age || gmgr.Limit() != int64(c.limit) {
		ok, err := askConfirmation("Existing Governor configuration does not match, update?", false)
		if err != nil {
			return err
		}

		if ok {
			err = gmgr.SetLimit(c.limit)
			if err != nil {
				return err
			}

			err = gmgr.SetMaxAge(c.age)
			if err != nil {
				return err
			}
		}
	}

	return c.viewAction(pc)
}

func (c *govCmd) viewAction(_ *kingpin.ParseContext) error {
	nc, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	gmgr, err := governor.NewJSGovernorManager(c.name, c.limit, c.age, c.replicas, mgr, false)
	if err != nil {
		return err
	}

	fmt.Printf("                 Name: %s\n", gmgr.Name())
	fmt.Printf("      Process Timeout: %v\n", gmgr.MaxAge())
	fmt.Printf("             Replicas: %d\n", gmgr.Replicas())

	act, err := gmgr.LastActive()
	if err != nil {
		return err
	}
	if act.IsZero() {
		fmt.Printf("           Last Entry: never\n")
	} else {
		fmt.Printf("           Last Entry: %v ago\n", time.Since(act).Round(time.Second))
	}

	fmt.Printf(" Active Process Limit: %d\n", gmgr.Limit())

	stream := gmgr.Stream()
	nfo, err := stream.LatestInformation()
	if err != nil {
		return err
	}
	fmt.Printf("     Active Processes: %d\n", nfo.State.Msgs)

	if nfo.State.Msgs > 0 {
		fmt.Println()
		table := newTableWriter("Active Processes")
		table.AddHeaders("ID", "Process Name", "Age")
		defer func() { fmt.Println(table.Render()) }()

		sub, err := nc.SubscribeSync(nats.NewInbox())
		if err != nil {
			return err
		}
		defer sub.Unsubscribe()

		_, err = stream.NewConsumer(jsm.DeliverySubject(sub.Subject), jsm.DeliverAllAvailable(), jsm.AcknowledgeNone())
		if err != nil {
			return err
		}

		for {
			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				return err
			}

			meta, err := jsm.ParseJSMsgMetadata(msg)
			if err != nil {
				continue
			}

			table.AddRow(meta.StreamSequence(), string(msg.Data), time.Since(meta.TimeStamp()).Round(time.Millisecond))

			if meta.Pending() == 0 {
				break
			}
		}
	}

	return nil
}

func (c *govCmd) resetAction(pc *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation("Reset the Governor?", false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("skipped")
		}
	}

	gmgr, err := governor.NewJSGovernorManager(c.name, c.limit, c.age, c.replicas, mgr, false)
	if err != nil {
		return err
	}

	return gmgr.Reset()
}

func (c *govCmd) evictAction(pc *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	gmgr, err := governor.NewJSGovernorManager(c.name, c.limit, c.age, c.replicas, mgr, false)
	if err != nil {
		return err
	}

	active, err := gmgr.Active()
	if err != nil {
		return err
	}
	if active == 0 {
		return fmt.Errorf("no slots in use")
	}

	if !c.force {
		err = c.viewAction(pc)
		if err != nil {
			return err
		}

		fmt.Println()

		ok, err := askConfirmation(fmt.Sprintf("Delete member %d?", c.id), false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("skipped")
		}
	}

	_, err = gmgr.Evict(c.id)
	return err
}

func (c *govCmd) runAction(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	if c.interval < 100*time.Millisecond {
		return fmt.Errorf("interval should be > 100 ms")
	}

	parts, err := shellquote.Split(c.command)
	if err != nil {
		return fmt.Errorf("can not parse command: %s", err)
	}
	var cmd string
	var args []string

	switch {
	case len(parts) == 0:
		return fmt.Errorf("could not parse command")
	case len(parts) == 1:
		cmd = parts[0]
	default:
		cmd = parts[0]
		args = append(args, parts[1:]...)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.age)
	defer cancel()

	gov := governor.NewJSGovernor(c.name, mgr, governor.WithInterval(c.interval))
	finisher, _, err := gov.Start(ctx, c.ident)
	if err != nil {
		return fmt.Errorf("could not get a execution slot: %s", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		finisher()
	}()

	osExit := func(c int, format string, a ...interface{}) {
		finisher()

		if format != "" {
			fmt.Println(fmt.Sprintf(format, a...))
		}

		os.Exit(c)
	}

	execution := exec.Command(cmd, args...)
	execution.Stdin = os.Stdin
	execution.Stdout = os.Stdout
	execution.Stderr = os.Stderr
	err = execution.Run()
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if ok {
			osExit(exitErr.ExitCode(), "")
		} else {
			osExit(1, "execution failed: %s", err)
		}
	}

	if execution.ProcessState == nil {
		osExit(1, "Unknown execution state")
	}

	osExit(execution.ProcessState.ExitCode(), "")

	return nil
}
