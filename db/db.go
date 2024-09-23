package db

import (
	"database/sql"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
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