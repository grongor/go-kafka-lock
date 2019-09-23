package kafkalock

import "time"

func newTestConfig() *Config {
	config := NewConfig([]string{""}, "loremLock")
	config.Logger = NewNopLogger()
	config.FlapGuard = NewNopFlapGuard()

	return config
}

func async(fn func()) <-chan bool {
	ch := make(chan bool)

	go func() {
		fn()
		ch <- true
		close(ch)
	}()

	go func() {
		time.Sleep(time.Second)
		select {
		case <-ch:
		default:
			close(ch)
		}
	}()

	return ch
}
