package storage

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
)

var (
	testDSN = os.Getenv("TEST_PG_URL")
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	if testDSN == "" {
		return m.Run()
	}

	u, err := url.Parse(testDSN)
	if err != nil {
		fmt.Println("parse dsn failed:", err)
		return 1
	}

	admin, err := sql.Open("postgres", testDSN)
	if err != nil {
		fmt.Println("open db failed:", err)
		return 1
	}
	defer admin.Close()

	_, err = admin.Exec("DROP DATABASE IF EXISTS wallet_service_test")
	if err != nil {
		fmt.Println("drop db failed:", err)
		return 1
	}

	_, err = admin.Exec("CREATE DATABASE wallet_service_test")
	if err != nil {
		fmt.Println("create db failed:", err)
		return 1
	}
	defer func() {
		_, err := admin.Exec("DROP DATABASE wallet_service_test")
		if err != nil {
			fmt.Println("drop db failed:", err)
		}
	}()

	u.Path = "/wallet_service_test"
	u.RawPath = ""

	db, err := sql.Open("postgres", u.String())
	if err != nil {
		fmt.Println("open test db failed:", err)
		return 1
	}
	defer db.Close()

	schema, err := os.ReadFile("../../init.sql")
	if err != nil {
		fmt.Println("read schema failed:", err)
		return 1
	}

	if _, err := db.Exec(string(schema)); err != nil {
		fmt.Println("execute schema failed:", err)
		return 1
	}

	testDSN = u.String()

	return m.Run()
}

func newTestStore(t *testing.T) *Store {
	t.Helper()

	if testDSN == "" {
		t.Skip("set TEST_PG_URL to run PostgreSQL integration tests")
	}

	store, err := NewStore(testDSN)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	})

	return store
}
