package cmd

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"

	"github.com/datamigrate/csv"
	"github.com/datamigrate/db"
	dm "github.com/datamigrate/migration"
	"github.com/datamigrate/utils"
	"github.com/golang-migrate/migrate/v4"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "datamigrate",
	Short: "Pin data migrations to your golang-migrate migrated db",
	Run: func(cmd *cobra.Command, args []string) {
		// Default action if no subcommand is provided
		// print help
		cmd.Help()
	},
}

// Define the 'up' subcommand
var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Run data migrations up",
	Run: func(cmd *cobra.Command, args []string) {
		m, version, err := connectAndCheckVersion(cmd)
		if err != nil {
			log.Fatalf("An error occurred: %v", err)
		}
		dbUrl := cmd.Flag("db-url").Value.String()
		// connect to the database with sql.Open
		conn, err := sql.Open("postgres", dbUrl)
		if err != nil {
			log.Fatalf("An error occurred while connecting to the database: %v", err)
		}
		// check if the data migration table exists
		dataMigrationTableExists := db.CheckDataMigrationTableExists(conn)
		if !dataMigrationTableExists {
			// create the data migration table
			err = db.CreateDataMigrationTable(conn)
			if err != nil {
				log.Fatalf("An error occurred while creating the data migration table: %v", err)
			}
		}

		// get the current datamigration version from the db
		currentDataMigrationVersion, err := db.GetVersion(conn)
		if err != nil {
			log.Printf("An error occurred while getting the current data migration version: %v", err)
		}

		if currentDataMigrationVersion == version {
			log.Printf("The current data migration version is the same as the target version: %v", version)
			return
		}

		// get all the data migrations

		fmt.Println("Current data migration version", currentDataMigrationVersion)

		// read the migrations from the data migrations directory
		dataMigrationsDir := cmd.Flag("datamigrations-dir").Value.String()
		dataMigrationsDirAbs, err := filepath.Abs(dataMigrationsDir)
		if err != nil {
			log.Fatalf("An error occurred while getting the absolute path of the data migrations directory: %v", err)
		}
		fmt.Println("Reading data migrations from", dataMigrationsDirAbs)
		dataMigrations, err := dm.ReadDataMigrations(dataMigrationsDirAbs)

		if err != nil {
			log.Fatalf("An error occurred while reading the data migrations: %v", err)
		}

		// fmt.Println("Data migrations", dataMigrations)

		availableVersions, err := dm.ParseVersions(dataMigrations)
		if err != nil {
			log.Fatalf("An error occurred while parsing the versions: %v", err)
		}
		fmt.Println("Available versions", availableVersions)

		// range over the available versions
		for _, targetVersion := range availableVersions {
			// find the data migration with the corresponding version
			dataMigration := dm.GetDataMigrationByVersion(dataMigrations, targetVersion)
			if dataMigration == nil {
				log.Fatalf("Data migration with version %d not found", targetVersion)
			}

			fmt.Printf("Data migration for version: %d %s\n", targetVersion, dataMigration)

			// load the csv
			c, err := csv.LoadCSV(dataMigration.CSVPath, dataMigration.Delimiter)
			if err != nil {
				log.Fatalf("An error occurred while loading the csv: %v", err)
			}
			// validate the csv columns against the migration columns
			err = csv.ValidateColumns(c, dataMigration)

			if err != nil {
				log.Fatalf("Column order mismatch: %v", err)
			}
			// load the csv to the database
			err = db.WriteCsvToDb(conn, c, dataMigration.Table)
			if err != nil {
				log.Fatalf("An error occurred while writing the csv to the database: %v", err)
			}

			db.SetVersion(conn, int(targetVersion))

			fmt.Println("Data migration completed successfully")

		}
		defer conn.Close()
		defer m.Close()

	},
}

// Define the 'down' subcommand
var downCmd = &cobra.Command{
	Use:   "down",
	Short: "Revert data migrations down",
	Run: func(cmd *cobra.Command, args []string) {
		m, _, err := connectAndCheckVersion(cmd)

		// check the data migration table exists
		dbUrl := cmd.Flag("db-url").Value.String()
		conn, err := sql.Open("postgres", dbUrl)

		currentDataMigrationVersion, err := db.GetVersion(conn)
		if err != nil {
			log.Fatalf("An error occurred while getting the current data migration version: %v", err)
		}
		if currentDataMigrationVersion == 0 {
			log.Printf("The current data migration version is 0. Nothing to do. Exiting...")
			return
		}

		fmt.Println("Current data migration version", currentDataMigrationVersion)

		// get all the data migrations
		dataMigrationsDir := cmd.Flag("datamigrations-dir").Value.String()
		dataMigrationsDirAbs, err := filepath.Abs(dataMigrationsDir)
		if err != nil {
			log.Fatalf("An error occurred while getting the absolute path of the data migrations directory: %v", err)
		}

		dataMigrations, err := dm.ReadDataMigrations(dataMigrationsDirAbs)
		if err != nil {
			log.Fatalf("An error occurred while reading the data migrations: %v", err)
		}
		// get the versions
		availableVersions, err := dm.ParseVersions(dataMigrations)
		fmt.Println("Available versions", availableVersions)
		// for range reversed over the available versions
		for i := len(availableVersions) - 1; i >= 0; i-- {
			v := availableVersions[i]
			fmt.Println("Truncating table for version", v)
			dataMigration := dm.GetDataMigrationByVersion(dataMigrations, v)
			if dataMigration == nil {
				log.Fatalf("Data migration with version %d not found", v)
			}
			fmt.Printf("Data migration for version: %d %s\n", v, dataMigration)
			db.TruncateTable(conn, dataMigration.Table)
			db.SetVersion(conn, int(v))

		}

		// set the version to 0
		db.SetVersion(conn, 0)
		fmt.Println("Data migrations reverted successfully")
		if err != nil {
			log.Fatalf("An error occurred: %v", err)
		}
		defer m.Close()

	},
}

// Define the 'create' subcommand
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new data migration",
	Run: func(cmd *cobra.Command, args []string) {
		// Implement the logic to create a new data migration
		dataMigrationsDir := cmd.Flag("datamigrations-dir").Value.String()
		// sql migrations dir
		sqlMigrationsDir := cmd.Flag("migrations-dir").Value.String()

		version := cmd.Flag("version").Value.String()

		if version == "" {
			log.Fatalf("The version is required")

		}
		if dataMigrationsDir == "" {
			log.Fatalf("The data migrations directory is required")

		}
		if sqlMigrationsDir == "" {
			log.Fatalf("The migrations directory is required")

		}

		// pad the version integer with zeros
		version = fmt.Sprintf("%06s", version)

		fmt.Println("Creating a new data migration with version", version)

		migrationDirAbs, err := filepath.Abs(dataMigrationsDir)
		if err != nil {
			log.Fatalf("An error occurred while getting the absolute path of the data migrations directory: %v", err)
		}
		fmt.Println("Creating a new data migration in", migrationDirAbs)

		// get all the files in the migrations directory
		migrationFiles, err := utils.GetMigrations(sqlMigrationsDir)
		if err != nil {
			log.Fatalf("An error occurred while getting the migrations: %v", err)
		}

		migrations := dm.ParseMigrationObjects(migrationFiles)
		// get the migration with the corresponding version
		migration := dm.GetMigrationByVersion(migrations, version, dm.Up)
		if migration == nil {
			log.Fatalf("The migration with version %s does not exist", version)
		}
		fmt.Println("Found migration", dm.PrettyPrintMigration(migration))

		// get the migration path for the data migration
		mPath := migration.GetBasePath()
		fmt.Println("Migration path", mPath)

		// create the dataMigration object
		mPath = filepath.Join(migrationDirAbs, fmt.Sprintf("%s_%s.yml", version, migration.Name))
		fmt.Println("Data migration path", mPath)
		dataMigration := &dm.DataMigration{
			Migration: migration,
			Path:      mPath,
		}

		// create the empty data migration files
		path, err := dm.CreateMigrationFile(dataMigration, migration)
		if err != nil {
			log.Fatalf("An error occurred while creating the data migration file: %v", err)
		}
		log.Printf("Data migration file created successfully at path %s", path)

	},
}

func init() {
	// Add a db url flag
	rootCmd.PersistentFlags().StringP("db-url", "d", "", "Database URL")
	// Add a migrations directory flag
	rootCmd.PersistentFlags().StringP("migrations-dir", "m", "", "Migrations directory")
	// Add a datamigrations directory flag
	rootCmd.PersistentFlags().String("datamigrations-dir", "", "Data Migrations directory")
	// Add subcommands: up, down, and create

	createCmd.Flags().StringP("version", "v", "", "The migration version to pin the datamigration to")
	rootCmd.AddCommand(upCmd)
	rootCmd.AddCommand(downCmd)
	rootCmd.AddCommand(createCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("An error occurred while executing the root command: %v", err)
	}
}

func connectAndCheckVersion(cmd *cobra.Command) (*migrate.Migrate, uint, error) {
	// Get the db url from the environment variables
	dbUrl := cmd.Flag("db-url").Value.String()

	// Connect to the database
	driver, err := db.ConnectDatabase(dbUrl)
	if err != nil {
		return nil, 0, fmt.Errorf("an error occurred while connecting to the database: %v", err)
	}

	sourceDir := cmd.Flag("migrations-dir").Value.String()
	if sourceDir == "" {
		return nil, 0, fmt.Errorf("the migrations directory is required")
	}

	sourceDirAbs := utils.GetAbsoluteSourceDir(sourceDir)

	m, err := migrate.NewWithDatabaseInstance(
		sourceDirAbs,
		"postgres", driver)

	if err != nil {
		return nil, 0, fmt.Errorf("an error occurred while creating the migration instance: %v", err)
	}

	// Get the current version
	version, dirty, err := m.Version()
	if err != nil {
		return nil, 0, fmt.Errorf("an error occurred while getting the current version: %v", err)
	}
	if dirty {
		return nil, 0, fmt.Errorf("the current version is dirty. Please fix state to continue")
	}

	return m, version, nil
}
