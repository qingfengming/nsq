package nsq_sync

import (
	"net"
	"os"
	"github.com/youzan/nsq/internal/protocol"
	"sync"
	"github.com/youzan/nsq/internal/util"
	"bytes"
)

type NsqSync struct {
	sync.RWMutex
	opts           *Options
	TcpListener    net.Listener
	tcpListener    net.Listener
	httpListener   net.Listener
	waitGroup      util.WaitGroupWrapper
	//sync target map from peer nsq topic sync
	targetTopicMap map[string]map[int]*TopicInfo
	//map of topic sync tasks
	inSyncTopicMap map[string]*TopicSync
}

type TopicInfo struct {
	topicName string
	partition int
	bp              sync.Pool
}

func (t *TopicInfo) BufferPoolGet(capacity int) *bytes.Buffer {
	b := t.bp.Get().(*bytes.Buffer)
	b.Reset()
	b.Grow(capacity)
	return b
}

func (t *TopicInfo) BufferPoolPut(b *bytes.Buffer) {
	t.bp.Put(b)
}

func NewTopicInfo(topicName string, partition int) *TopicInfo{
	ti := &TopicInfo{
		topicName:topicName,
		partition:partition,
	}
	ti.bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
	return ti
}

func New(opts *Options) *NsqSync {
	n := &NsqSync{
		opts: opts,
		targetTopicMap:make(map[string]map[int]*TopicInfo),
	}
	return n
}

func (l *NsqSync) getTopic(topicName string, partition int) *TopicInfo {
	l.RLock()
	pars, ok := l.targetTopicMap[topicName]
	if ok {
		ti, ok := pars[partition]
		if ok {
			l.RLock()
			return ti
		}
	}
	l.RUnlock()

	l.Lock()
	pars, ok = l.targetTopicMap[topicName]
	if ok {
		ti, ok := pars[partition]
		if ok {
			l.RLock()
			return ti
		}
	} else {
		pars = make(map[int]*TopicInfo)
		l.targetTopicMap[topicName] = pars
	}

	if partition < 0 {
		partition = 0
	}
	t := NewTopicInfo(topicName, partition)
	pars[partition] = t
	nsqSyncLog.Logf("TOPIC(%s:%v): created", t.topicName, t.partition)
	l.Unlock()

	return t
}

func (l *NsqSync) Main() {
	ctx := &context{
		nsqsync: l,
	}
	var err error

	ctx.tcpAddr, err = net.ResolveTCPAddr("tcp", l.opts.TCPAddress)
	if err != nil {
		nsqSyncLog.Errorf("fail to parse nsqproxy TCP address %v", l.opts.TCPAddress)
		return;
	}
	nsqSyncLog.Infof("nsq proxy TCP address: %v", ctx.tcpAddr.String())

	ctx.peerTcpAddr, err = net.ResolveTCPAddr("tcp", l.opts.PeerNSQSyncTCPAddr)
	if err != nil {
		nsqSyncLog.Errorf("fail to parse nsqproxy forward TCP address %v", l.opts.PeerNSQSyncTCPAddr)
		os.Exit(1)
	}
	nsqSyncLog.Infof("nsq proxy forward TCP address: %v", ctx.peerTcpAddr.String())

	ctx.httpAddr, err = net.ResolveTCPAddr("tcp", l.opts.HTTPAddress)
	if err != nil {
		nsqSyncLog.Errorf("fail to parse nsqproxy HTTP address %v", l.opts.HTTPAddress)
		os.Exit(1)
	}
	nsqSyncLog.Infof("nsq proxy HTTP address: %v", ctx.httpAddr.String())

	ctx.peerHttpAddr, err = net.ResolveTCPAddr("tcp", l.opts.PeerNSQSyncHTTPAddr)
	if err != nil {
		nsqSyncLog.Errorf("fail to parse peer nsqproxy TCP address %v", l.opts.PeerNSQSyncHTTPAddr)
		os.Exit(1)
	}
	nsqSyncLog.Infof("peer nsq proxy HTTP address: %v", ctx.peerHttpAddr.String())

	//tcp server
	TcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		nsqSyncLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.TcpListener = TcpListener
	ctx.tcpAddr = TcpListener.Addr().(*net.TCPAddr)
	l.Unlock()
	nsqSyncLog.Logf("TCP: listening on %s", TcpListener.Addr())
	tcpServer := &tcpServer{ctx: ctx}
	//init topic producer
	err = InitProducerManager(l.opts)
	if err != nil {
		nsqSyncLog.Errorf("fail to initialize topic producer manager, err: %v", err)
		os.Exit(1)
	}

	l.waitGroup.Wrap(func() {
		protocol.TCPServer(TcpListener, tcpServer)
		nsqSyncLog.Logf("TCP: closing %s", TcpListener.Addr())
	})

	////TODO: HTTP server
	//httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	//ctx.httpAddr = httpListener.Addr().(*net.TCPAddr)
	//if err != nil {
	//	nsqSyncLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.HTTPAddress, err)
	//	os.Exit(1)
	//}
	//l.Lock()
	//l.httpListener = httpListener
	//l.Unlock()
	//httpServer := newHTTPServer(ctx, false, l.opts.TLSRequired == nsqd.TLSRequired)
	//l.waitGroup.Wrap(func() {
	//	http_api.Serve(httpListener, httpServer, "HTTP", l.opts.Logger)
	//})
}

func (l *NsqSync) RealTCPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	if l.tcpListener == nil || l.tcpListener.Addr() == nil {
		return nil
	}
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NsqSync) RealHTTPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	if l.httpListener == nil || l.httpListener.Addr() == nil {
		return nil
	}
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NsqSync) Exit() {
	nsqSyncLog.Logf("nsqproxy server stopping.")

	if l.tcpListener != nil {
		l.tcpListener.Close()
	}
	if l.TcpListener != nil {
		l.TcpListener.Close()
	}
	if l.httpListener != nil {
		l.httpListener.Close()
	}

	l.waitGroup.Wait()
	nsqSyncLog.Logf("nsqproxy stopped.")
}