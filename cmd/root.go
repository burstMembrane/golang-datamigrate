package cmd

import (
	"fmt"
	"log"

	"github.com/golang-datamigrate/db"
	"github.com/golang-datamigrate/types"
	"github.com/golang-datamigrate/utils"
	"github.com/golang-migrate/migrate/v4"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "golang-datamigrate",
    Short: "Pin data migrations to your golang-migrate migrated db",
	Run: func(cmd *cobra.Command, args []string) {

    },
}

func Execute() {
    // Add a db url flag
    rootCmd.PersistentFlags().StringP("db-url", "d", "", "Database URL")
    // Add a migrations directory flag
    rootCmd.PersistentFlags().StringP("migrations-dir", "m", "", "Migrations directory")
    // Add a datamigrations directory flag
    rootCmd.PersistentFlags().String("datamigrations-dir", "database/datamigrations", "Data Migrations directory")

    if err := rootCmd.Execute(); err != nil {
        log.Fatalf("An error occurred while executing the root command: %v", err)
    }

    // Get the db url from the environment variables
    dbUrl := rootCmd.Flag("db-url").Value.String()

    // Connect to the database
    driver, err := db.ConnectDatabase(dbUrl)
    if err != nil {
        log.Fatalf("An error occurred while connecting to the database: %v", err)
    }

    sourceDir := rootCmd.Flag("migrations-dir").Value.String()
    if sourceDir == "" {
        log.Fatalf("The migrations directory is required")
    }

    sourceDirAbs := utils.GetAbsoluteSourceDir(sourceDir)

    m, err := migrate.NewWithDatabaseInstance(
        sourceDirAbs,
        "postgres", driver)

    if err != nil {
        log.Fatalf("An error occurred while creating the migration instance: %v", err)
    }

    // Get all the migrations in the migrations directory
    migrations, err := utils.GetMigrations(sourceDir)
    if err != nil {
        log.Fatalf("An error occurred while getting the migrations: %v", err)
    }

    // Parse the migrations to objects
    migrationObjects := types.ParseMigrationObjects(migrations)

    // Call the pretty print function
    for _, migration := range migrationObjects {
        fmt.Println(types.PrettyPrintMigration(migration))
    }
 
	
	// Print the current version
    version, dirty, err := m.Version()
    if err != nil {
        log.Fatalf("An error occurred while getting the current version: %v", err)
    }
    if dirty {
        log.Fatalf("The current version is dirty. Please fix state to continue")
    }
    log.Printf("Current version: %v, Dirty: %v", version, dirty)
	
	
	latestMigration := types.GetLatest(migrationObjects)
	if latestMigration == nil {
		log.Fatalf("The latest migration could not be found.")
	}
	// check the last migration equals the version
	
	if version != types.GetMigrationVersion(latestMigration) {
		log.Printf("Database Version: %d\n Last Migration: %d \n database is not up to date, please migrate with gomigrate before running this tool", version, types.GetMigrationVersion(latestMigration))
		return
	}
	
}