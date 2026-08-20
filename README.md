<h1 align="center"> GoDis-KV: In-Memory Key-Value Store </h1>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go">
  <img src="https://img.shields.io/badge/Architecture-TCP%20Server-333333?style=for-the-badge" alt="TCP Server">
  <img src="https://img.shields.io/badge/Status-Em%20Evolu%C3%A7%C3%A3o-success?style=for-the-badge" alt="Status">
</p>

## Sobre o Projeto (System Design)

O **GoDis-KV** é um banco de dados Chave-Valor em memória, construído do zero em Go e fortemente inspirado na arquitetura do Redis. 

Este projeto atua como uma **Prova de Conceito (PoC) em evolução contínua**, projetada para aprofundar conhecimentos críticos em engenharia de software backend: concorrência em alta escala, comunicação de rede de baixo nível (TCP) e mecanismos de persistência de dados. O repositório está em constante desenvolvimento, servindo como base para futuras implementações de sistemas distribuídos.


## Decisões Arquiteturais e Padrões Aplicados

Para garantir baixa latência e alta disponibilidade, a arquitetura foge de frameworks prontos e foca na linguagem pura:

### 1. Servidor TCP Customizado
Ao invés de utilizar protocolos HTTP tradicionais que adicionam overhead de cabeçalhos, o servidor foi construído nativamente utilizando o pacote `net` do Go. Isso garante uma comunicação direta baseada em streams de bytes, maximizando o throughput de leitura e escrita.

### 2. Concorrência e Thread-Safety
Bancos em memória sofrem com condições de corrida (Race Conditions) sob alta carga. O núcleo de dados utiliza `sync.RWMutex` acoplado aos mapas em Go.
* **O ganho:** Permite múltiplas rotinas de leitura simultâneas, mas bloqueia o acesso de forma segura e atômica durante operações de escrita ou deleção, garantindo a consistência do estado.

### 3. Persistência AOF e Graceful Shutdown
Bancos em memória perdem dados ao desligar. Para mitigar isso, foi implementado o padrão AOF (Append-Only File).
* Cada operação de mutação de estado é logada em disco sequencialmente. 
* O servidor escuta sinais do sistema operacional (SIGINT/SIGTERM) para executar um *Graceful Shutdown*: fecha conexões TCP ativas, processa as requisições pendentes e garante o *flush* dos dados no disco antes de encerrar o processo.

### 4. Motor de Evicção (TTL - Time-To-Live)
Sistema de expiração de chaves gerenciado por *Goroutines* em background, que varrem passivamente o dicionário limpando dados com o ciclo de vida expirado, liberando memória automaticamente.

## Qualidade de Software e Cobertura

O projeto foi desenhado considerando cenários de estresse e concorrência:
* **Benchmarking Integrado:** Uma ferramenta de teste de carga customizada (`cmd/bench`) desenvolvida para estressar o servidor principal, medindo operações por segundo e latência média.
* **Testes de Concorrência:** Validação de comportamento sob alto volume de goroutines para atestar a ausência de *data races* durante leitura/escrita simultânea.

## Como Executar Localmente

### Servidor Principal (Daemon)
Inicia o banco de dados e aguarda conexões na porta TCP padrão:
```bash
go run cmd/server/main.go

```

### Cliente de Benchmark (Load Testing)

Em um terminal separado, execute a ferramenta de estresse para validar a resiliência do servidor:

```bash
go run cmd/bench/main.go

```


## Autor

**Augusto Ortigoso Barbosa**

* **GitHub:** [github.com/supp3rguto](https://github.com/supp3rguto)
* **LinkedIn:** [linkedin.com/in/oaugustobarbosa](https://www.linkedin.com/in/oaugustobarbosa/)
