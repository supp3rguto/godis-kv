# godis-kv

Banco de dados **Key-Value em memória** construído do zero em Go, operando sobre protocolo TCP puro e sem dependências externas.

A proposta do projeto é sair da camada de frameworks web de alto nível e descer para a "sala de máquinas" da computação — construindo um clone minimalista do Redis para armazenar sessões ou cache de alta velocidade, lidando diretamente com sockets TCP, concorrência segura em memória, persistência em disco e encerramento controlado de processos.

## O que está sendo praticado

Mais do que implementar um mapa em memória, cada pacote do repositório exercita um fundamento central de engenharia de sistemas e Go idiomático:

- **Rede em baixo nível (`net` / TCP raw):** servidor TCP customizado escutando conexões brutas na porta `:6379` (configurável via CLI), sem HTTP ou frameworks web, com parser próprio de comandos de texto.
- **Concorrência e Thread-Safety (`goroutines` + `sync.RWMutex`):** cada cliente conectado é atendido em sua própria goroutine; leituras simultâneas (`GET`) compartilham o lock de leitura (`RLock`), enquanto escritas e remoções (`SET`, `SETEX`, `DEL`) aplicam bloqueio exclusivo (`Lock`) para eliminar *race conditions*.
- **Expiração de chaves (TTL):** suporte a chaves com tempo de vida útil combinando verificação passiva na leitura (`Get`) e uma rotina ativa em background (`time.NewTicker`) que remove chaves expiradas periodicamente da memória.
- **Persistência simples via Append-Only File (AOF):** cada operação de mutação efetiva é gravada sequencialmente em disco (`data/database.log`). Ao reiniciar o servidor, o método `Replay` lê o arquivo e reconstrói o estado na memória.
- **Desacoplamento por interfaces (Go idiomático):** o pacote `internal/server` define os contratos que consome (`KeyValueStore` e `AppendLogger`), sem importar `internal/storage` diretamente — permitindo injetar *mocks* e testar o protocolo sem tocar no disco.
- **Graceful Shutdown:** interceptação dos sinais `SIGINT` (`Ctrl+C`) e `SIGTERM` via `os/signal` para encerrar o listener TCP, impedir novas conexões e fechar o arquivo AOF com segurança antes de finalizar o processo.
- **Testes automatizados com Race Detector:** suíte de testes cobrindo CRUD, expiração real de TTL, estresse concorrente com 1.000 goroutines simultâneas, replay de AOF com `t.TempDir()` e protocolo TCP em memória com `net.Pipe()`.

## Stack e ferramentas

- Go `1.26` (**100% biblioteca padrão**, zero dependências de terceiros)
- Pacotes nativos: `net`, `bufio`, `sync`, `os/signal`, `flag`, `testing`
- Go Race Detector (`go test -race`)
- `netcat` (`nc`) para interação manual via TCP no terminal

## Estrutura do projeto

```text
godis-kv/
├── .gitignore
├── go.mod
├── cmd/
│   ├── server/
│   │   └── main.go                 # Ponto de entrada: flags CLI (-addr, -aof), replay do AOF e graceful shutdown
│   └── bench/
│       └── main.go                 # Script de carga disparando 10.000 requisições TCP concorrentes
└── internal/
    ├── server/
    │   ├── tcp.go                  # Servidor TCP raw, parser de comandos e interfaces KeyValueStore / AppendLogger
    │   └── tcp_test.go             # Testes do protocolo TCP via net.Pipe() usando mocks em memória
    └── storage/
        ├── db.go                   # Motor Key-Value em memória, sync.RWMutex e rotina de limpeza de TTL
        ├── db_test.go              # Testes de CRUD, expiração de TTL e concorrência (1.000 workers)
        ├── aof.go                  # Persistência Append-Only File (criação automática de diretório, Append e Replay)
        └── aof_test.go             # Testes de escrita, reabertura e Replay com t.TempDir()
```

## Nota sobre arquitetura e persistência

O pacote `internal/server` foi estruturado seguindo a convenção de Go *"accept interfaces, return structs"*. Por definir `KeyValueStore` e `AppendLogger` no próprio pacote consumidor, o servidor TCP não conhece detalhes de implementação do mapa em memória nem do sistema de arquivos, o que possibilitou testar todo o protocolo com conexões em memória (`net.Pipe`) e validar exatamente quais comandos geram escrita no log.

Para evitar falhas de inicialização em clones limpos do repositório (já que o diretório `data/` e os arquivos `*.log` são ignorados no `.gitignore`), o construtor `storage.NewAOF` cria automaticamente os diretórios necessários via `os.MkdirAll` antes de abrir o arquivo. No estágio atual, o AOF opera apenas em modo append; a compactação periódica (`BGREWRITEAOF`) está mapeada nos próximos passos.

## Como executar localmente

### Pré-requisitos

- Go `1.22+` instalado (`go version`)
- `netcat` (`netcat-openbsd` no Linux) para enviar comandos manuais via TCP

### Passo a passo

1. Clone o repositório e acesse o diretório:

```bash
git clone https://github.com/supp3rguto/godis-kv.git
cd godis-kv
```

2. **Terminal 1** — suba o servidor TCP (porta padrão `:6379` e log em `data/database.log`):

```bash
go run ./cmd/server
```

Também é possível customizar porta e arquivo AOF via flags:

```bash
go run ./cmd/server -addr :6380 -aof data/custom.log
```

Para compilar o binário executável:

```bash
go build -o bin/godis-server ./cmd/server
./bin/godis-server -addr :6379 -aof data/database.log
```

3. **Terminal 2** — conecte-se ao banco via TCP puro com `netcat`:

```bash
nc localhost 6379
```

### Rodando os testes e o detector de Race Conditions

Execute toda a suíte com verificação de *race conditions* e relatório de cobertura:

```bash
go test -race -v -cover ./...
```

Cobertura atual dos pacotes internos:
- `internal/storage`: `92.6%`
- `internal/server`: `68.2%`

### Rodando o benchmark de carga

Com o servidor rodando no **Terminal 1** na porta `:6379`, dispare 10.000 requisições concorrentes (`SET` e `GET`) a partir de outro terminal:

```bash
go run ./cmd/bench
```

## Protocolo e comandos

| Comando | Sintaxe | Persistido no AOF | Respostas |
| --- | --- | --- | --- |
| `SET` | `SET <chave> <valor>` | Sim | `OK`, `ERR Sintaxe: SET <chave> <valor>` |
| `SETEX` | `SETEX <chave> <segundos> <valor>` | Sim | `OK`, `ERR Segundos invalidos`, `ERR Sintaxe: SETEX <chave> <segundos> <valor>` |
| `GET` | `GET <chave>` | Não | `<valor>`, `(nil)`, `ERR Sintaxe: GET <chave>` |
| `DEL` | `DEL <chave>` | Sim (se existir) | `1` (removido), `0` (inexistente), `ERR Sintaxe: DEL <chave>` |

Valores com espaços são suportados tanto no `SET` quanto no `SETEX`. Comandos não reconhecidos retornam `ERR Comando desconhecido`.

### Exemplos via `netcat`

```text
$ nc localhost 6379
SET usuario guto
OK
GET usuario
guto
SET frase banco em memoria com go
OK
GET frase
banco em memoria com go
SETEX sessao 5 token_temporario
OK
GET sessao
token_temporario
# após 5 segundos:
GET sessao
(nil)
DEL usuario
1
GET usuario
(nil)
```

## Progresso e próximos passos

- [x] Servidor TCP customizado com pacote `net` nativo e goroutine por conexão
- [x] Engine Key-Value em memória protegida com `sync.RWMutex`
- [x] Persistência em disco com Append-Only File (AOF) e `Replay` no boot
- [x] Expiração de chaves (`SETEX`) com limpeza passiva e ativa em background
- [x] Graceful Shutdown interceptando `SIGINT` e `SIGTERM`
- [x] Refatoração com interfaces (`KeyValueStore` e `AppendLogger`) e flags CLI (`-addr`, `-aof`)
- [x] Testes unitários e de integração com `-race` (`db_test.go`, `aof_test.go`, `tcp_test.go`)
- [x] Script inicial de benchmark concorrente (`cmd/bench/main.go`)
- [ ] Compactação do AOF (`BGREWRITEAOF`) com escrita em arquivo temporário e troca atômica (`os.Rename`)
- [ ] Mensageria em tempo real (`SUBSCRIBE` / `PUBLISH`) usando Go Channels
- [ ] Evoluir o benchmark para calcular latência média, P95 e P99 e registrar os resultados neste README
- [ ] Adicionar `Makefile` na raiz para padronizar `build`, `run`, `test` e `bench`

## Autor

Augusto Ortigoso Barbosa

- GitHub: https://github.com/supp3rguto
- LinkedIn: https://www.linkedin.com/in/augusto-barbosa-769602194
