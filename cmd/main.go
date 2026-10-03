package main

import (
	"io"
	"log"
	"net"
	"time"
)

func main() {
	upstreamAddr := "localhost:9000"

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("Error starting server:", err)
	}
	defer ln.Close()

	for {
		client, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go handleClientConnection(client, upstreamAddr)
	}
}

func handleClientConnection(client net.Conn, upstreamAddr string) {
	upstream, err := net.DialTimeout("tcp", upstreamAddr, 10*time.Second)
	if err != nil {
		log.Printf("Error connecting to upstream %s: %v", upstreamAddr, err)
		client.Close()
		return
	}

	log.Printf("proxying %s <-> %s", client.RemoteAddr(), upstream.RemoteAddr())

	results := make(chan error, 2)
	go copyStream(upstream, client, results) // client -> upstream
	go copyStream(client, upstream, results) // upstream -> client

	// Force both connections closed so the OTHER blocked goroutine's
	// Read() unblocks with an error instead of hanging forever.
	firstErr := <-results
	log.Printf("first direction finished: %v", firstErr)

	if firstErr != nil {
		// real error — nothing to wait for, close now
		client.Close()
		upstream.Close()
		secondErr := <-results
		log.Printf("second direction finished: %v", secondErr)
	} else {
		// clean EOF — the other direction might still be relaying data, let it finish naturally
		secondErr := <-results
		log.Printf("second direction finished: %v", secondErr)
		client.Close()
		upstream.Close()
	}

	log.Printf("connection fully closed: %s <-> %s", client.RemoteAddr(), upstream.RemoteAddr())

	log.Printf("connection fully closed: %s <-> %s", client.RemoteAddr(), upstream.RemoteAddr())
}

func copyStream(destination, source net.Conn, results chan<- error) {
	_, err := io.Copy(destination, source)

	if err == nil {
		// source hit EOF cleanly — that's what we're calling "EOF came"
		if tcpConn, ok := destination.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
	}
	// any real error falls through here without CloseWrite — handled differently in handleClientConnection

	results <- err
}
