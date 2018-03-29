package nsq_sync

import (
	"testing"
	"time"
	"sync"
	"github.com/youzan/go-nsq"
	"strings"
)

func InitNSQSyncProxy(opts *Options) *NsqSync {
	sync := New(opts)
	return sync
}

func TestProtocolSyncPub(t *testing.T) {
	opts1 := NewOptions()
	opts1.ReadTimeout = 3*time.Second
	opts1.WriteTimeout = 3*time.Second
	opts1.PeerNSQSyncTCPAddr = "0.0.0.0:4151"
	opts1.NSQLookupdHttpAddress = "127.0.0.1:4161"
	sync1 := InitNSQSyncProxy(opts1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sync1.Main()
		wg.Done()
	}()
	wg.Wait()

	//start a publish and publish
	pCfg := nsq.NewConfig()
	producer, err := nsq.NewProducer("127.0.0.1:4150", pCfg)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	err = producer.Publish("JavaTesting-Order", []byte("message content"))
	if err != nil && !strings.Contains(err.Error(), "topic producer not found") {
		t.Fail()
	} else if err == nil{
		t.Errorf("err should not be nil")
	}
	sync1.Exit()
}