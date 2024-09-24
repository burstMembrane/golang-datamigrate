package db

import (
	"database/sql"
	"fmt"

	"github.com/golang-datamigrate/csv"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/lib/pq"
)

func ConnectDatabase(dsn string) (database.Driver, error) {
	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return nil, err
	}
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return nil, err
	}
	return driver, nil
}
func CheckDataMigrationTableExists(db *sql.DB) bool {
	// Check if the data migration table exists
	err := db.Ping()
	if err != nil {
		return false
	}

	_, err = db.Exec(`SELECT 1 FROM schema_datamigrations LIMIT 1;`)
	if err != nil {
		return err == nil
	}

	return true
}

func CreateDataMigrationTable(db *sql.DB) error {
	// Create the data migration table
	err := db.Ping()
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_datamigrations (
			version bigint NOT NULL,
			dirty boolean NOT NULL,
			CONSTRAINT schema_datamigrations_pkey PRIMARY KEY (version)
		);`)
	if err != nil {
		return err
	}

	return nil
}

func DropDataMigrationTable(db *sql.DB) error {
	// Drop the data migration table
	err := db.Ping()
	if err != nil {
		return err
	}

	_, err = db.Exec(`DROP TABLE IF EXISTS schema_datamigrations;`)
	if err != nil {
		return err
	}

	return nil
}
func TruncateTable(db *sql.DB, tableName string) error {
	// Truncate the table
	err := db.Ping()
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(`TRUNCATE TABLE %s;`, tableName))
	if err != nil {
		return err
	}

	return nil
}

// WriteCsvToDb copies the CSV data into the database using PostgreSQL COPY command.
func WriteCsvToDb(db *sql.DB, csv *csv.CSV, tableName string) error {
	// Begin a transaction

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Prepare the COPY statement
	stmt, err := tx.Prepare(pq.CopyIn(tableName, csv.Columns...))
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	// Iterate over the rows and execute the COPY statement
	for _, row := range csv.Rows {
		values := make([]interface{}, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		fmt.Println(values)
		_, err = stmt.Exec(values...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	// Signal completion of COPY
	_, err = stmt.Exec()
	if err != nil {
		tx.Rollback()
		return err
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func GetVersion(db *sql.DB) (uint, error) {
	// Get the current version from the data migration table
	err := db.Ping()
	if err != nil {
		return 0, err
	}

	var version uint
	err = db.QueryRow(`SELECT version FROM schema_datamigrations ORDER BY version DESC LIMIT 1;`).Scan(&version)
	if err != nil {
		if err == sql.ErrNoRows {
			// No rows means no migrations have been run yet
			return 0, nil
		}
		return 0, err
	}
	// convert to uint

	return version, nil
}

func SetVersion(db *sql.DB, version int) error {
	// Ensure the database connection is alive
	err := db.Ping()
	if err != nil {
		return err
	}
	// Truncate the table
	_, err = db.Exec(`TRUNCATE TABLE schema_datamigrations;`)
	if err != nil {
		return err
	}

	// Perform the insert operation
	_, err = db.Exec(`
        INSERT INTO schema_datamigrations (version, dirty) 
        VALUES ($1, false);
    `, version)
	if err != nil {
		return err
	}

	return nil
}
func RemoveVersion(db *sql.DB, version int) error {
	// Remove the version from the data migration table
	err := db.Ping()
	if err != nil {
		return err
	}

	_, err = db.Exec(`DELETE FROM schema_datamigrations WHERE version = $1;`, version)
	if err != nil {
		return err
	}

	return nil
}

func SetDirty(db *sql.DB, version int) error {
	// Set the dirty flag in the data migration table
	err := db.Ping()
	if err != nil {
		return err
	}

	_, err = db.Exec(`UPDATE schema_datamigrations SET dirty = true WHERE version = $1;`, version)
	if err != nil {
		return err
	}

	return nil
}

func ClearDirty(db *sql.DB, version int) error {
	// Clear the dirty flag in the data migration table
	err := db.Ping()
	if err != nil {
		return err
	}

	_, err = db.Exec(`UPDATE schema_datamigrations SET dirty = false WHERE version = $1;`, version)
	if err != nil {
		return err
	}

	return nil
}
