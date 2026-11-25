package pubsub

import "sync"

const defaultBufferSize = 64

// broker gerencia canais e assinantes de pub/sub em memoria de forma concorrente.
type Broker struct {
	mu       sync.RWMutex
	channels map[string]map[chan string]struct{}
}

func NewBroker() *Broker {
	return &Broker{
		channels: make(map[string]map[chan string]struct{}),
	}
}

// subscribe registra novo assinante no canal e retorna o channel de leitura junto com uma função idempotente para cancelar a inscrica
func (b *Broker) Subscribe(channel string) (<-chan string, func()) {
	ch := make(chan string, defaultBufferSize)

	b.mu.Lock()
	if _, exists := b.channels[channel]; !exists {
		b.channels[channel] = make(map[chan string]struct{})
	}
	b.channels[channel][ch] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			if subs, exists := b.channels[channel]; exists {
				delete(subs, ch)
				if len(subs) == 0 {
					delete(b.channels, channel)
				}
			}
			close(ch)
		})
	}

	return ch, unsubscribe
}

// Publish envia a mensagem para todos os assinantes do canal sem bloquear
// caso algum cliente esteja lento (buffer cheio). Retorna quantos receberam.
func (b *Broker) Publish(channel, message string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subs, exists := b.channels[channel]
	if !exists {
		return 0
	}

	delivered := 0
	for ch := range subs {
		select {
		case ch <- message:
			delivered++
		default:
			// Assinante lento: descarta sem travar o publisher
		}
	}

	return delivered
}