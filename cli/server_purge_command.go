package cli

import (
	"fmt"

	"github.com/choria-io/fisk"
)

type SrvPurgeCmd struct {
	force   bool
	account string
}

func configureServerPurgeCommand(srv *fisk.CmdClause) {
	c := &SrvPurgeCmd{}

	raft := srv.Command("purge", "Purge assets from JetStream clusters")

	account := raft.Command("account", "Purge all data from an account").Action(c.purgeAccount)
	account.Arg("account", "The name of the account to purge").PlaceHolder("NAME").Required().StringVar(&c.account)
	account.Flag("force", "Perform the operation without prompting").Short('f').UnNegatableBoolVar(&c.force)
}

func (c *SrvPurgeCmd) purgeAccount(_ *fisk.ParseContext) error {
	if !c.force {
		fmt.Printf("This operation deletes all data from the %s account and cannot be reversed.\n\n", c.account)
		remove, err := askConfirmation(fmt.Sprintf("Really purge account %s", c.account), false)
		if err != nil {
			return err
		}

		if !remove {
			return nil
		}
	}

	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	err = mgr.MetaPurgeAccount(c.account)
	if err != nil {
		return err
	}

	fmt.Printf("Purge operation on account %s initiated\n", c.account)

	return nil
}
