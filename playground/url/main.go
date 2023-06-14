package main

import (
	"fmt"
	"net/url"
)

func main() {
	urlStr := "test://user:pass@localhost:3800/test?some=setting"
	u, err := url.Parse(urlStr)
	if err != nil {
		panic(err)
	}

	printUrl(u)

	urlStr = "test://user:pass@localhost:3800,localhost:3801/test?some=setting"
	u, err = url.Parse(urlStr)
	if err != nil {
		panic(err)
	}

	printUrl(u)
}

func printUrl(u *url.URL) {
	fmt.Printf("Scheme: %v\n", u.Scheme)
	fmt.Printf("Host: %v\n", u.Host)
	fmt.Printf("Path: %v\n", u.Path)
	fmt.Printf("Port: %v\n", u.Port())
	fmt.Printf("User: %v\n", u.User.Username())
	password, _ := u.User.Password()
	fmt.Printf("Password: %v\n", password)
	fmt.Printf("RawQuery: %v\n", u.RawQuery)
	fmt.Printf("Query some: %v\n", u.Query().Get("some"))
}
