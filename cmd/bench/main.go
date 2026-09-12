 package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const (
	address    = "localhost:6379"
	numClients = 10000 // quantidade de requisições concorrentes
)

func main() {
	var wg sync.WaitGroup
	start := time.Now()

	fmt.Printf("Disparando %d requisições simultâneas contra %s...\n", numClients, address)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		
		// Inicia uma Goroutine para cada cliente
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", address)
			if err != nil {
				log.Printf("Erro ao conectar: %v", err)
				return
			}
			defer conn.Close()

			// aqui ele alterna entre comandos SET e GET para simular carga real
			if id%2 == 0 {
				cmd := fmt.Sprintf("SET user:%d ativo\n", id)
				conn.Write([]byte(cmd))
			} else {
				cmd := fmt.Sprintf("GET user:%d\n", id-1)
				conn.Write([]byte(cmd))
			}
			
			// leitura da resposta para garantir que o ciclo completou (tipo um buffer simples)
			buffer := make([]byte, 1024)
			conn.Read(buffer)
		}(i)
	}

	wg.Wait() // aguarda todas as 10k goroutines terminarem
	duration := time.Since(start)

	fmt.Printf("Benchmark concluído!\n")
	fmt.Printf("Tempo total: %v\n", duration)
	fmt.Printf("Requisições por segundo (estimado): %.2f req/sec\n", float64(numClients)/duration.Seconds())
}