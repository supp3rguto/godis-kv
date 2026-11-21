package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/supp3rguto/godis-kv/internal/server"
	"github.com/supp3rguto/godis-kv/internal/storage"
)

func main() {
	addr := flag.String("addr", ":6379", "Endereço e porta do servidor TCP")
	aofPath := flag.String("aof", "data/database.log", "Caminho do arquivo AOF de persistência")
	flag.Parse()

	aof, err := storage.NewAOF(*aofPath)
	if err != nil {
		log.Fatalf("Erro ao iniciar AOF: %v", err)
	}

	db := storage.NewDB()
	fmt.Printf("Carregando dados de %s...\n", *aofPath)
	if err := aof.Replay(db); err != nil {
		log.Fatalf("Erro ao carregar dados do AOF: %v", err)
	}

	srv := server.NewServer(*addr, db, aof)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Erro no servidor: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	fmt.Println("\nSinal recebido. Iniciando desligamento gracioso...")
	srv.Stop()
	if err := aof.Close(); err != nil {
		log.Printf("Erro ao fechar AOF: %v", err)
	}
	fmt.Println("Conexões fechadas e disco sincronizado. Adeus!")
}