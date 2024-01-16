package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
)

type Data struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type GetResponse struct {
	Data     Data `json:"data"`
	CacheHit bool `json:"cache_hit"`
}

type PutResponse struct {
	Data    Data `json:"data"`
	Created bool `json:"created"`
}

var dbRead *sql.DB
var dbWrite *sql.DB
var cache *redis.ClusterClient

func main() {
	var err error
	dbRead, err = sql.Open("mysql", os.Getenv("READ_REPLICA_DSN")) // Use the READ_REPLICA_DSN environment variable
	if err != nil {
		panic(err)
	}

	dbWrite, err = sql.Open("mysql", os.Getenv("WRITE_REPLICA_DSN")) // Use the WRITE_REPLICA_DSN environment variable
	if err != nil {
		panic(err)
	}

	redisAddresses := strings.Split(os.Getenv("REDIS_CLUSTER_ADDRESSES"), ",") // Split the REDIS_CLUSTER_ADDRESSES environment variable by comma
	redisPassword := os.Getenv("REDIS_PASSWORD")                               // Use the REDIS_PASSWORD environment variable
	cache = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    redisAddresses,
		Password: redisPassword,
	})

	r := mux.NewRouter()
	r.HandleFunc("/data/{id}", GetData).Methods("GET")
	r.HandleFunc("/data/{id}", PutData).Methods("PUT")
	r.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	http.ListenAndServe(":8080", r)
}

func GetData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, _ := strconv.Atoi(vars["id"])
	ctx := context.Background()
	cacheHit := true
	var data Data
	val, err := cache.Get(ctx, fmt.Sprintf("data:%d", id)).Result()
	if err == redis.Nil {
		cacheHit = false
		row := dbRead.QueryRow("SELECT * FROM data WHERE id = ?", id)
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
		cache.Set(ctx, fmt.Sprintf("data:%d", id), val, 0)
	} else if err != nil {
		panic(err)
	} else {
		json.Unmarshal([]byte(val), &data)
	}

	dataResponse := GetResponse{
		CacheHit: cacheHit,
		Data:     data,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dataResponse)
}

func PutData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, _ := strconv.Atoi(vars["id"])
	ctx := context.Background()

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
	cache.Set(ctx, fmt.Sprintf("data:%d", id), val, 0)

	putResponse := PutResponse{
		Created: rowsAffected == 1,
		Data:    data,
	}

	if rowsAffected == 1 {
		w.WriteHeader(http.StatusCreated) // 201 Created for new resource
	} else if rowsAffected == 2 {
		w.WriteHeader(http.StatusOK) // 200 OK for updated resource
	}

	val, _ = json.Marshal(putResponse)
	w.Header().Set("Content-Type", "application/json")
	w.Write(val)
}
