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

func init() {
	// Add a database flag
	rootCmd.PersistentFlags().StringP("conn", "c", "", "Database URL")
	// Add a path flag
	rootCmd.PersistentFlags().StringP("path", "p", "", "Migrations directory")
	// Add a datapath flag
	rootCmd.PersistentFlags().StringP("datapath", "d", "", "Data Migrations directory")
	// Add subcommands: up, down, and create

	createCmd.Flags().StringP("version", "v", "", "The migration version to pin the datamigration to")

	// add example
	rootCmd.Example = `datamigrate up -c "postgres://localhost:5432/<db-name>" -p "./migrations" -d "./datamigrations"`
	// add example for create
	createCmd.Example = `datamigrate create -v "000001" -p "./migrations" -d "./datamigrations"`
	rootCmd.AddCommand(upCmd)
	rootCmd.AddCommand(downCmd)
	rootCmd.AddCommand(createCmd)
}

// Define the 'up' subcommand
var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Run data migrations up",
	Run: func(cmd *cobra.Command, args []string) {
		dbUrl := cmd.Flag("conn").Value.String()
		// read the migrations from the data migrations directory
		dataMigrationsDir := cmd.Flag("datapath").Value.String()

		m, version, err := connectAndCheckVersion(cmd)
		if err != nil {
			log.Fatalf("An error occurred: %v", err)
		}

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

		log.Println("Current data migration version", currentDataMigrationVersion)

		dataMigrationsDirAbs, err := filepath.Abs(dataMigrationsDir)
		if err != nil {
			log.Fatalf("An error occurred while getting the absolute path of the data migrations directory: %v", err)
		}
		log.Println("Reading data migrations from", dataMigrationsDirAbs)
		dataMigrations, err := dm.ReadDataMigrations(dataMigrationsDirAbs)

		if err != nil {
			log.Fatalf("An error occurred while reading the data migrations: %v", err)
		}

		// log.Println("Data migrations", dataMigrations)

		availableVersions, err := dm.ParseVersions(dataMigrations)
		if err != nil {
			log.Fatalf("An error occurred while parsing the versions: %v", err)
		}
		// range over the available versions
		for _, targetVersion := range availableVersions {
			// find the data migration with the corresponding version
			dataMigration := dm.GetDataMigrationByVersion(dataMigrations, targetVersion)
			if dataMigration == nil {
				log.Fatalf("Data migration with version %d not found", targetVersion)
			}

			fmt.Printf("Running migration file for version: %d %s\n", targetVersion, dataMigration.CSVPath)

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

			log.Println("Data migration completed successfully")

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
		if err != nil {
			log.Fatalf("An error occurred: %v", err)
		}
		// check the data migration table exists
		dbUrl := cmd.Flag("conn").Value.String()
		// get all the data migrations
		dataMigrationsDir := cmd.Flag("datapath").Value.String()

		conn, err := sql.Open("postgres", dbUrl)
		if err != nil {
			log.Fatalf("An error occurred while connecting to the database: %v", err)
		}

		currentDataMigrationVersion, err := db.GetVersion(conn)
		if err != nil {
			log.Fatalf("An error occurred while getting the current data migration version: %v", err)
		}
		if currentDataMigrationVersion == 0 {
			log.Printf("The current data migration version is 0. Nothing to do. Exiting...")
			return
		}

		log.Println("Current data migration version", currentDataMigrationVersion)

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
		log.Println("Available versions", availableVersions)
		// for range reversed over the available versions
		for i := len(availableVersions) - 1; i >= 0; i-- {
			v := availableVersions[i]
			log.Println("Truncating table for version", v)
			dataMigration := dm.GetDataMigrationByVersion(dataMigrations, v)
			if dataMigration == nil {
				log.Fatalf("Data migration with version %d not found", v)
			}
			fmt.Printf("Loading file for version: %d %s\n", v, dataMigration.CSVPath)
			db.TruncateTable(conn, dataMigration.Table)
			db.SetVersion(conn, int(v))

		}

		// set the version to 0
		db.SetVersion(conn, 0)
		log.Println("Data migrations reverted successfully")
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
		dataMigrationsDir := cmd.Flag("datapath").Value.String()
		// sql migrations dir
		sqlMigrationsDir := cmd.Flag("path").Value.String()

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

		log.Println("Creating a new data migration with version", version)

		migrationDirAbs, err := filepath.Abs(dataMigrationsDir)
		if err != nil {
			log.Fatalf("An error occurred while getting the absolute path of the data migrations directory: %v", err)
		}
		log.Println("Creating a new data migration in", migrationDirAbs)

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
		log.Println("Found migration", dm.PrettyPrintMigration(migration))

		// get the migration path for the data migration
		mPath := migration.GetBasePath()
		log.Println("Migration path", mPath)

		// create the dataMigration object
		mPath = filepath.Join(migrationDirAbs, fmt.Sprintf("%s_%s.yml", version, migration.Name))
		log.Println("Data migration path", mPath)
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

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("An error occurred while executing the root command: %v", err)
	}
}

func connectAndCheckVersion(cmd *cobra.Command) (*migrate.Migrate, uint, error) {
	// Get the db url from the environment variables
	dbUrl := cmd.Flag("conn").Value.String()

	// Connect to the database
	driver, err := db.ConnectDatabase(dbUrl)
	if err != nil {
		return nil, 0, fmt.Errorf("an error occurred while connecting to the database: %v", err)
	}

	sourceDir := cmd.Flag("path").Value.String()
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
