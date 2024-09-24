package main

import (
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/datamigrate/cmd"
	_ "github.com/lib/pq"
)

func main() {
	cmd.Execute()
}
