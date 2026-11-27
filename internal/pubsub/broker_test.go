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
		t.Fatalf("want 0 deliveries, got %d", count)
	}

	ch1, unsub1 := broker.Subscribe("alertas")
	ch2, unsub2 := broker.Subscribe("alertas")
	defer unsub2()

	if count := broker.Publish("alertas", "cpu_alta"); count != 2 {
		t.Fatalf("want 2 deliveries, got %d", count)
	}

	for i, ch := range []<-chan string{ch1, ch2} {
		select {
		case msg := <-ch:
			if msg != "cpu_alta" {
				t.Errorf("subscriber %d: want 'cpu_alta', got %q", i+1, msg)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message on subscriber %d", i+1)
		}
	}

	unsub1()
	unsub1()

	if _, open := <-ch1; open {
		t.Fatal("subscriber 1 channel should be closed after unsubscribe")
	}

	if count := broker.Publish("alertas", "disco_cheio"); count != 1 {
		t.Fatalf("want 1 delivery after unsubscribe, got %d", count)
	}

	select {
	case msg := <-ch2:
		if msg != "disco_cheio" {
			t.Errorf("subscriber 2: want 'disco_cheio', got %q", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for second message on subscriber 2")
	}
}

func TestBroker_NonBlockingOnSlowSubscriber(t *testing.T) {
	broker := NewBroker()
	_, unsub := broker.Subscribe("stream")
	defer unsub()

	for i := 0; i < defaultBufferSize; i++ {
		if count := broker.Publish("stream", fmt.Sprintf("msg-%d", i)); count != 1 {
			t.Fatalf("expected delivery %d to succeed, got %d", i, count)
		}
	}

	if count := broker.Publish("stream", "overflow"); count != 0 {
		t.Fatalf("expected 0 deliveries when buffer is full, got %d", count)
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

func BenchmarkBroker_Publish(b *testing.B) {
	for _, subs := range []int{0, 1, 10} {
		b.Run(fmt.Sprintf("subscribers=%d", subs), func(b *testing.B) {
			broker := NewBroker()
			var wg sync.WaitGroup
			unsubs := make([]func(), 0, subs)

			for i := 0; i < subs; i++ {
				ch, unsub := broker.Subscribe("bench")
				unsubs = append(unsubs, unsub)
				wg.Add(1)
				go func(c <-chan string) {
					defer wg.Done()
					for range c {
					}
				}(ch)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				broker.Publish("bench", "benchmark_event_payload")
			}
			b.StopTimer()

			for _, unsub := range unsubs {
				unsub()
			}
			wg.Wait()
		})
	}
}