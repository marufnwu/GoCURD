package main

import (
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	"log"
)

func main() {
	//connect to a database
	conn, err := sql.Open("pgx", "host=localhost port=5432 dbname=test_connect user=postgres password=123456")
	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to connect: %v\n", err))
	}
	defer conn.Close()

	log.Println("Connected to database")

	//test my connection
	err = conn.Ping()
	if err != nil {
		log.Fatal("Cannot ping database")
	}

	log.Println("Successfully ping to database")

	//get rows from table
	err = getAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}

	//insert a row

	query := `INSERT INTO users (first_name, last_name) VALUES($1, $2)`

	_, err = conn.Exec(query, "JACK", "BROWN")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Inserted a row")
	defer conn.Close()

	//get rows from  table  again
	err = getAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}

	//update a row
	stmt := `UPDATE users set first_name=$1 WHERE first_name=$2`

	_, err = conn.Exec(stmt, "JACKIE", "JACK")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Updated a row")
	defer conn.Close()

	//get rows from table again
	err = getAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}

	//get one row by id

	stmt = `SELECT id, first_name, last_name from users WHERE id=$1`

	res := conn.QueryRow(stmt, 2)
	var firstName, lastName string
	var id int

	err = res.Scan(&id, &firstName, &lastName)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Select a row by id: ", id, firstName, lastName)
	log.Println("Get a row by id")
	defer conn.Close()

	//delete a row
	stmt = `DELETE from users WHERE id=$1`

	_, err = conn.Exec(stmt, 3)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Deleted  a row")
	defer conn.Close()

	//get row again
	err = getAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}

}

func getAllRows(conn *sql.DB) error {
	rows, err := conn.Query("SELECT id, first_name, last_name from users")
	if err != nil {
		log.Fatal(err)
		return err
	}

	defer rows.Close()

	var firstName, lastName string
	var id int
	for rows.Next() {
		err := rows.Scan(&id, &firstName, &lastName)
		if err != nil {
			log.Println(err)
			return err
		}

		fmt.Println("Record is", id, firstName, lastName)
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Error caning error", err)
	}

	fmt.Println("===========================")

	return nil
}
