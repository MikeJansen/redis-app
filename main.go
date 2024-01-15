package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

type Data struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var dbRead *sql.DB
var dbWrite *sql.DB
var cache *redis.ClusterClient

func main() {
	var err error
	dbRead, err = sql.Open("mysql", "user:password@tcp(read-replica-host:3306)/dbname")
	if err != nil {
		panic(err)
	}

	dbWrite, err = sql.Open("mysql", "user:password@tcp(write-replica-host:3306)/dbname")
	if err != nil {
		panic(err)
	}

	cache = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{"localhost:6379", "localhost:6380", "localhost:6381"},
		Password: "",
	})

	r := mux.NewRouter()
	r.HandleFunc("/data/{id}", GetData).Methods("GET")
	r.HandleFunc("/data/{id}", PutData).Methods("PUT")

	http.ListenAndServe(":8080", r)
}

func GetData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, _ := strconv.Atoi(vars["id"])

	val, err := cache.Get(fmt.Sprintf("data:%d", id)).Result()
	if err == redis.Nil {
		row := dbRead.QueryRow("SELECT * FROM data WHERE id = ?", id)
		var data Data
		err = row.Scan(&data.ID, &data.Name)
		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "Not Found", http.StatusNotFound)
				return
			}
			panic(err)
		}

		valBytes, _ := json.Marshal(data)
		val = string(valBytes)
		cache.Set(fmt.Sprintf("data:%d", id), val, 0)
	} else if err != nil {
		panic(err)
	}

	var data Data
	json.Unmarshal([]byte(val), &data)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func PutData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, _ := strconv.Atoi(vars["id"])

	var data Data
	json.NewDecoder(r.Body).Decode(&data)

	res, err := dbWrite.Exec("INSERT INTO data (id, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = ?", id, data.Name, data.Name)
	if err != nil {
		panic(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		panic(err)
	}

	val, _ := json.Marshal(data)
	cache.Set(fmt.Sprintf("data:%d", id), val, 0)

	if rowsAffected == 1 {
		w.WriteHeader(http.StatusCreated) // 201 Created for new resource
	} else if rowsAffected == 2 {
		w.WriteHeader(http.StatusOK) // 200 OK for updated resource
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(val)
}
