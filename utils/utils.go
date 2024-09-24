package utils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
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

// deprecated - attempts to source DB_URL from .env
// TODO: remove in later version
func FetchDBURL() string {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	dbUrl, isFound := os.LookupEnv("DB_URL")
	if !isFound {
		log.Fatal("DB_URL not found in .env file")
	}
	return dbUrl
}
