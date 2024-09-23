package types

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

type migrationType string

const (
	Up   migrationType = "up"
	Down migrationType = "down"
)


type Migration struct {
	version       string
	name          string
	migrationType migrationType
	path          string
}


type DataMigration struct {
	migration *Migration
	path string
}


func GetMigrationVersion(migration *Migration) uint {
	
	var migrationVersion uint
	fmt.Sscanf(migration.version, "%d", &migrationVersion)
	
	return migrationVersion
}

func PrettyPrintMigration(migration *Migration) string {
	return fmt.Sprintf("<Migration: Version: %s, Name: %s, Type: %s />", migration.version, migration.name, migration.migrationType)
}


func GetLatest(migrations []*Migration) *Migration {
	// get the migration with the highest version by parsing the version string to an integer
	var lastMigration *Migration
	for _, migration := range migrations {
		if lastMigration == nil {
			lastMigration = migration
			continue
		}
		if migration.version > lastMigration.version {
			lastMigration = migration
		}
	}
	return lastMigration
}


func ParseMigrationObjects(migrations []string) []*Migration {
	var migrationObjects []*Migration
	for _, migration := range migrations {

		m, err := toMigration(migration)
		if err != nil {
			log.Fatalf("An error occurred while parsing the migration: %v", err)
			os.Exit(1)
		}
		migrationObjects = append(migrationObjects, m)

	}
	return migrationObjects
}




func toMigration(migrationPath string) (*Migration, error) {

	var re = regexp.MustCompile(`(\d{6})_([a-z_]+)\.([a-z]+)`)

	matches := re.FindStringSubmatch(filepath.Base(migrationPath))
	if len(matches) != 4 {
		return nil, fmt.Errorf("Migration file %s does not match the expected pattern", migrationPath)
	}

	version := matches[1]
	name := matches[2]
	mtype := matches[3]
	if mtype != "up" && mtype != "down" {
		return nil, fmt.Errorf("Migration file %s does not match the expected pattern", migrationPath)
	}	
	var migrationType migrationType
	if mtype == "up" {
		migrationType = Up
	} else {
		migrationType = Down
	}
	
	return &Migration{
		version:       version,
		name:          name,
		migrationType: migrationType,
	}, nil
}

