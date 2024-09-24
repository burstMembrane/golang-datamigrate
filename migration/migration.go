package types

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/auxten/postgresql-parser/pkg/walk"
	"gopkg.in/yaml.v2"
)

type MigrationType string

const (
	Up   MigrationType = "up"
	Down MigrationType = "down"
)

type Migration struct {
	Version       string
	Name          string
	MigrationType MigrationType
	Path          string
}

type DataMigration struct {
	Migration *Migration

	Path string
}

type Column struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

type MigrationDDL struct {
	Version   string   `yaml:"version"`
	CSVPath   string   `yaml:"csv_path"`
	Delimiter string   `yaml:"delimiter"`
	Pre       string   `yaml:"pre"`
	Post      string   `yaml:"post"`
	Table     string   `yaml:"table_name"`
	Columns   []Column `yaml:"columns"`
}

func (m *Migration) GetBasePath() string {
	// get the migration's base path without the .sql extension
	return fmt.Sprintf("%s_%s", m.Version, m.Name)
}

func ParseVersions(dataMigrations *[]MigrationDDL) ([]int, error) {
	var versions []int
	for _, migration := range *dataMigrations {
		v, err := strconv.Atoi(migration.Version)
		if err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, nil
}

func GetDataMigrationByVersion(dataMigrations *[]MigrationDDL, version int) *MigrationDDL {
	for _, migration := range *dataMigrations {
		v, err := strconv.Atoi(migration.Version)
		if err != nil {
			log.Fatalf("An error occurred while parsing the version: %v", err)
		}
		if v == version {
			return &migration
		}
	}
	return nil
}

func GetMigrationVersion(migration *Migration) uint {

	var migrationVersion uint
	fmt.Sscanf(migration.Version, "%d", &migrationVersion)
	return migrationVersion
}

func (m MigrationDDL) Format() string {
	var result string
	result += fmt.Sprintf("table: %s\n", m.Table)
	result += "columns:\n"
	for _, col := range m.Columns {
		result += fmt.Sprintf("    name: %s \n    type: %s\n", col.Name, col.Type)
	}
	return result
}

func ToYaml(migration *Migration) []byte {

	buf, err := os.ReadFile(migration.Path)
	if err != nil {
		log.Fatalf("An error occurred while reading the migration file: %v", err)
	}
	// read to string
	sql := string(buf)

	var tableName string
	var columns []Column
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		w := &walk.AstWalker{
			Fn: func(ctx interface{}, node interface{}) (stop bool) {
				switch n := node.(type) {
				case *tree.CreateTable:
					tableName = n.Table.TableName.String()
					log.Printf("CREATE TABLE %s", tableName)
				case *tree.ColumnTableDef:
					column := Column{
						Name: n.Name.String(),
						Type: n.Type.String(),
					}
					columns = append(columns, column)
					log.Printf("Column %s %s", column.Name, column.Type)
				}
				return false
			},
		}

		stmts, err := parser.Parse(sql)
		if err != nil {
			log.Fatalf("An error occurred while parsing the SQL: %v", err)
		}

		_, _ = w.Walk(stmts, nil)
	}()

	wg.Wait()
	w := &walk.AstWalker{
		Fn: func(ctx interface{}, node interface{}) (stop bool) {

			switch n := node.(type) {
			case *tree.CreateTable:

				tableName = n.Table.TableName.String()
				log.Printf("CREATE TABLE %s", tableName)
			case *tree.ColumnTableDef:
				column := Column{
					Name: n.Name.String(),
					Type: n.Type.String(),
				}
				// if the column is not already in the columns array, then append it
				// to avoid duplicate columns
				for _, col := range columns {
					if col.Name == column.Name {
						return false
					}
				}
				columns = append(columns, column)
				log.Printf("Column %s %s", column.Name, column.Type)
			}
			return false
		},
	}

	stmts, err := parser.Parse(sql)
	if err != nil {
		log.Fatalf("An error occurred while parsing the SQL: %v", err)
	}

	_, _ = w.Walk(stmts, nil)
	fmt.Println("Table Name: ", tableName)

	m := MigrationDDL{
		Version:   migration.Version,
		CSVPath:   "",
		Delimiter: ",",
		Table:     tableName,
		Columns:   columns,
	}

	yaml, err := yaml.Marshal(m)
	if err != nil {
		log.Fatalf("An error occurred while marshalling the migration to yaml: %v", err)
	}
	fmt.Println(string(yaml))

	return yaml

}

func ReadDataMigrations(migrationsDirPath string) (*[]MigrationDDL, error) {
	// open the directory
	dir, err := os.Open(migrationsDirPath)

	if err != nil {
		return nil, err
	}
	defer dir.Close()

	// read the files in the directory
	files, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}

	// parse the files
	var migrations []MigrationDDL
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		// read the file
		path := filepath.Join(migrationsDirPath, file.Name())

		migration, err := ReadMigrationFile(path)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, *migration)
	}
	return &migrations, nil

}

func ReadMigrationFile(path string) (*MigrationDDL, error) {

	// Read the file
	file, err := os.Open(path)

	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Parse the yaml
	var migration MigrationDDL
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&migration)
	if err != nil {
		return nil, err
	}

	// check if the csv file exists
	if _, err := os.Stat(migration.CSVPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("the csv file %s does not exist", migration.CSVPath)
	}

	return &migration, nil
}

func CreateMigrationFile(d *DataMigration, m *Migration) (string, error) {
	// Check if the file already exists

	// create the directory if it does not exist
	if _, err := os.Stat(filepath.Dir(d.Path)); os.IsNotExist(err) {
		err := os.MkdirAll(filepath.Dir(d.Path), os.ModePerm)
		if err != nil {
			return "", err
		}
	}
	if _, err := os.Stat(d.Path); !os.IsNotExist(err) {

		// debug: delete the file
		os.Remove(d.Path)
		// return "", fmt.Errorf("the data migration file already exists")
	}
	// convert the CREATE TABLE statement to yaml
	yml := ToYaml(m)

	// Create the file
	file, err := os.Create(d.Path)
	if err != nil {
		return "", err
	}

	// write the yaml to the file
	_, err = file.Write(yml)
	if err != nil {
		return "", err
	}
	defer file.Close()

	return d.Path, nil
}

func PrettyPrintMigration(migration *Migration) string {
	return fmt.Sprintf("<Migration: Version: %s, Name: %s, Type: %s />", migration.Version, migration.Name, migration.MigrationType)
}

func GetMigrationByVersion(migrations []*Migration, version string, mtype MigrationType) *Migration {
	for _, migration := range migrations {
		if migration.Version == version && migration.MigrationType == mtype {
			return migration
		}
	}
	return nil
}
func GetLatest(migrations []*Migration) *Migration {
	// get the migration with the highest version by parsing the version string to an integer
	var lastMigration *Migration
	for _, migration := range migrations {
		if lastMigration == nil {
			lastMigration = migration
			continue
		}
		if migration.Version > lastMigration.Version {
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
	var migrationType MigrationType
	if mtype == "up" {
		migrationType = Up
	} else {
		migrationType = Down
	}

	return &Migration{
		Version:       version,
		Name:          name,
		MigrationType: migrationType,
		Path:          migrationPath,
	}, nil
}
