package csv

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	dm "github.com/datamigrate/migration"
	"github.com/schollz/progressbar/v3"
)

type Row struct {
	Values []string
}
type CSV struct {
	Path      string
	Delimiter string
	Columns   []string
	Rows      []Row
}

func countLines(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lineCount, nil
}

func LoadCSV(path string, delimiter string) (*CSV, error) {
	// Load CSV file

	// get the abspath relative the cwd
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("an error occurred while getting the absolute path of the csv file: %v", err)
	}
	log.Println("Loading csv from path: ", absPath)
	file, err := os.Open(absPath)
	if err != nil {
		return nil, fmt.Errorf("an error occurred while opening the file: %v", err)
	}
	defer file.Close()

	totalLines, err := countLines(absPath)
	if err != nil {
		return nil, fmt.Errorf("an error occurred while counting the lines in the file: %v", err)
	}
	bar := progressbar.Default(int64(totalLines), "Loading CSV from path: "+path)

	reader := bufio.NewReader(file)
	index := 0

	var csvFile CSV
	csvFile.Path = absPath
	csvFile.Delimiter = delimiter

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("an error occurred while reading the file: %v", err)
		}

		// trim the line to remove any trailing newline characters
		line = strings.TrimSpace(line)

		// skip empty lines
		if line == "" {
			if err == io.EOF {
				break
			}
			continue
		}

		if index == 0 {
			// if it's the first line, then it's the header
			columns := strings.Split(line, delimiter)
			// trim the columns
			for i, col := range columns {
				columns[i] = strings.TrimSpace(col)
			}
			csvFile.Columns = columns
		} else {
			// create a row and append it to the rows
			row := Row{}
			row.Values = strings.Split(line, delimiter)

			// if the row isn't empty, then append it
			if len(row.Values) > 0 {
				csvFile.Rows = append(csvFile.Rows, row)
			}
		}

		index++
		bar.Add(1)

		if err == io.EOF {
			break
		}
	}

	return &csvFile, nil
}

func ValidateColumns(c *CSV, m *dm.MigrationDDL) error {
	// check the column order to match the migration
	migrationNames := []string{}
	for _, col := range m.Columns {
		migrationNames = append(migrationNames, col.Name)
	}

	areEqual := reflect.DeepEqual(c.Columns, migrationNames)
	if !areEqual {
		return fmt.Errorf("CSV Columns are not equal to columns in the original migration. CSV Columns: %v, Migration Columns: %v", c.Columns, migrationNames)
	}

	return nil
}
