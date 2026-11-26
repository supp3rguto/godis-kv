package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/supp3rguto/godis-kv/internal/pubsub"
	"github.com/supp3rguto/godis-kv/internal/server"
	"github.com/supp3rguto/godis-kv/internal/storage"
)

func main() {
	addr := flag.String("addr", ":6379", "TCP server bind address and port")
	aofPath := flag.String("aof", "data/database.log", "Path to the AOF persistence file")
	flag.Parse()

	aof, err := storage.NewAOF(*aofPath)
	if err != nil {
		log.Fatalf("Failed to initialize AOF: %v", err)
	}

	db := storage.NewDB()
	fmt.Printf("Loading data from %s...\n", *aofPath)
	if err := aof.Replay(db); err != nil {
		log.Fatalf("Failed to replay AOF: %v", err)
	}

	broker := pubsub.NewBroker()
	srv := server.NewServer(*addr, db, aof, broker)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	fmt.Println("\nShutdown signal received. Starting graceful shutdown...")
	srv.Stop()
	if err := aof.Close(); err != nil {
		log.Printf("Failed to close AOF: %v", err)
	}
	fmt.Println("Connections closed and disk synced. Goodbye!")
}