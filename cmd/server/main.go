package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/supp3rguto/godis-kv/internal/server"
	"github.com/supp3rguto/godis-kv/internal/storage"
)

func main() {
	aof, err := storage.NewAOF("data/database.log")
	if err != nil {
		log.Fatalf("Erro ao iniciar AOF: %v", err)
	}

	db := storage.NewDB()
	fmt.Println("Carregando dados do disco...")
	aof.Replay(db)

	srv := server.NewServer(":6379", db, aof)

	// roda o servidor em uma Goroutine separada para não travar a thread principal
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Erro no servidor: %v", err)
		}
	}()

	// cria um canal que escuta por sinais de interrupção do s.o. (ctrl c kill)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// A execução trava aqui até que um sinal chegue no canal 'stop'
	<-stop

	fmt.Println("\nSinal recebido. Iniciando desligamento gracioso...")
	
	srv.Stop() // fecha porta TCP e em baixo salva e fecha o arquivo de log no disco com segurança
	aof.Close()
	
	fmt.Println("Conexões fechadas e disco sincronizado. Adeus!")
}