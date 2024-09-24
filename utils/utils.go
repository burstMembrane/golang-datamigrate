package utils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func GetAbsoluteSourceDir(sourceDir string) string {
	// parse the source directory to get the absolute path
	sourceDir, err := filepath.Abs(sourceDir)
	if err != nil {
		log.Fatalf("An error occurred while getting the absolute path of the migrations directory: %v", err)
		os.Exit(1)
	}
	sourceDir = fmt.Sprintf("file://%v", sourceDir)

	return sourceDir
}

func GetMigrations(source_dir string) ([]string, error) {
	// Get all the migrations in the source directory
	dir := filepath.Clean(source_dir)
	ext := "." + strings.TrimPrefix("sql", ".")

	migrationFiles, err := filepath.Glob(filepath.Join(dir, "*"+ext))

	if err != nil {
		return nil, err
	}

	return migrationFiles, nil
}
