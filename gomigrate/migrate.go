package gomigrate

import (
	"database/sql"
	"errors"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"os"
)

// Migrate is only a helper that is used in the current local dev environment, so that
// I can spin up a database with the current migrations
func Migrate() error {

	url := os.Getenv("PG_CONN")

	db, err := sql.Open("postgres", url+"?sslmode=disable")
	if err != nil {
		return err
	}
	defer db.Close()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	pwd, _ := os.Getwd()
	println(pwd)

	m, err := migrate.NewWithDatabaseInstance("file://migrations", "postgres", driver)
	if err != nil {
		return err
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil

}
