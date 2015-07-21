package main

import "net/http"

func main() {

	s := NewHttpServer()

	http.HandleFunc("/LatestOffset", s.LatestHandler)
	http.HandleFunc("/ConsumerOffset", s.ConsumerHandler)

	http.ListenAndServe(":8088", nil)
}
