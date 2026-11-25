package pubsub

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBroker_PublishSubscribeAndUnsubscribe(t *testing.T) {
	broker := NewBroker()

	if count := broker.Publish("alertas", "ninguem"); count != 0 {
		t.Fatalf("esperava 0 entregas, recebeu %d", count)
	}

	ch1, unsub1 := broker.Subscribe("alertas")
	ch2, unsub2 := broker.Subscribe("alertas")
	defer unsub2()

	if count := broker.Publish("alertas", "cpu_alta"); count != 2 {
		t.Fatalf("esperava 2 entregas, recebeu %d", count)
	}

	for i, ch := range []<-chan string{ch1, ch2} {
		select {
		case msg := <-ch:
			if msg != "cpu_alta" {
				t.Errorf("assinante %d: esperava 'cpu_alta', recebeu %q", i+1, msg)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout aguardando mensagem no assinante %d", i+1)
		}
	}

	// valida idempotencia do unsubscribe
	unsub1()
	unsub1()

	if _, open := <-ch1; open {
		t.Fatal("channel do assinante 1 deveria estar fechado apos unsubscribe")
	}

	if count := broker.Publish("alertas", "disco_cheio"); count != 1 {
		t.Fatalf("esperava 1 entrega apos unsubscribe, recebeu %d", count)
	}

	select {
	case msg := <-ch2:
		if msg != "disco_cheio" {
			t.Errorf("assinante 2: esperava 'disco_cheio', recebeu %q", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout aguardando segunda mensagem no assinante 2")
	}
}

func TestBroker_NonBlockingOnSlowSubscriber(t *testing.T) {
	broker := NewBroker()
	_, unsub := broker.Subscribe("stream")
	defer unsub()

	for i := 0; i < defaultBufferSize; i++ {
		if count := broker.Publish("stream", fmt.Sprintf("msg-%d", i)); count != 1 {
			t.Fatalf("esperava entrega %d bem-sucedida, recebeu %d", i, count)
		}
	}

	if count := broker.Publish("stream", "overflow"); count != 0 {
		t.Fatalf("esperava 0 com buffer cheio, recebeu %d", count)
	}
}

func TestBroker_Concurrency(t *testing.T) {
	broker := NewBroker()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(2)

		go func(id int) {
			defer wg.Done()
			ch, unsub := broker.Subscribe("eventos")
			defer unsub()
			select {
			case <-ch:
			case <-time.After(20 * time.Millisecond):
			}
		}(i)

		go func(id int) {
			defer wg.Done()
			broker.Publish("eventos", fmt.Sprintf("evento-%d", id))
		}(i)
	}

	wg.Wait()
}