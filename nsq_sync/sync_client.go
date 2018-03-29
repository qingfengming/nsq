package nsq_sync

import (
	"net"
	"crypto/tls"
	"bufio"
	"sync"
	"sync/atomic"
	"time"
	"github.com/youzan/nsq/nsqd"
	"github.com/golang/snappy"
	"github.com/youzan/go-nsq"
)

const defaultBufferSize = 4 * 1024

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

type SyncClient struct {
	//nsqproxy peer connection, should be one
	net.Conn
	//TODO tls connection
	ID   int64
	opts *Options
	ClientID string
	Hostname string
	ExitChan chan(int)
	ConnectTime time.Time
	State int32
	tlsConfig *tls.Config

	Reader *bufio.Reader
	Writer *bufio.Writer

	// this lock used only for connection writer
	// do not use it while get/set stats for client, use meta lock instead
	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	lenBuf   [4]byte
	LenSlice []byte

	heartbeatInterval int64

	UserAgent string

	//identify data presenting nsq client behind peer nsqproxy
	identityData *nsqd.IdentifyDataV2

	TLS     int32
	Snappy  int32
	Deflate int32

	outputBufferSize    int64
	outputBufferTimeout int64
}

func NewSyncClient(id int64, peerConn net.Conn, opts *Options, tls *tls.Config) *SyncClient {
	var identifier string
	if peerConn != nil {
		identifier = peerConn.RemoteAddr().String()
	}
	if tcpC, ok := peerConn.(*net.TCPConn); ok {
		tcpC.SetNoDelay(true)
	}

	c := &SyncClient{
		ID:      id,
		opts: opts,

		Conn: peerConn,

		Reader: nsqd.NewBufioReader(peerConn),
		Writer: nsqd.NewBufioWriterSize(peerConn, defaultBufferSize),

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		State:          stateInit,

		ClientID: identifier,
		Hostname: identifier,

		// heartbeats are client configurable but default to 30s
		tlsConfig:         tls,

		outputBufferSize:    int64(defaultBufferSize),
		outputBufferTimeout: int64(250 * time.Millisecond),
	}
	c.LenSlice = c.lenBuf[:]
	return c
}

func (c *SyncClient) GetOutputBufferSize() int64 {
	return atomic.LoadInt64(&c.outputBufferSize)
}

func (c *SyncClient) GetOutputBufferTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&c.outputBufferTimeout))
}

func (c *SyncClient) Identify(data *nsqd.IdentifyDataV2) error {
	nsqSyncLog.Logf("[%s]-%v IDENTIFY: %+v", c, c.ID, data)
	c.SetIdentityData(data)
	return nil
}

func (c *SyncClient) GetHeartbeatInterval() time.Duration {
	return time.Duration(atomic.LoadInt64(&c.heartbeatInterval))
}

func (c *SyncClient) SetHeartbeatInterval(desiredInterval int) error {
	switch {
	case desiredInterval == -1:
		atomic.StoreInt64(&c.heartbeatInterval, 0)
	case desiredInterval == 0:
	// do nothing (use default)

	default:
		atomic.StoreInt64(&c.heartbeatInterval, int64(desiredInterval))
	}

	return nil
}

func (c *SyncClient) FinalClose() {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	if c.Reader != nil {
		nsqd.PutBufioReader(c.Reader)
		c.Reader = nil
	}
	if c.Writer != nil {
		nsqd.PutBufioWriter(c.Writer)
		c.Writer = nil
	}
	//TODO tls
	//if c.tlsConn != nil {
	//	c.tlsConn.Close()
	//	c.tlsConn = nil
	//}
	c.Conn.Close()
}

func (c *SyncClient) Flush() error {
	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	//TODO: what is flatWriter?
	//if c.flateWriter != nil {
	//	return c.flateWriter.Flush()
	//}

	return nil
}

func (c *SyncClient) UpgradeSnappy() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	//TODO tls connedtion
	//if c.tlsConn != nil {
	//	conn = c.tlsConn
	//}

	c.Reader = nsqd.NewBufioReader(snappy.NewReader(conn))
	c.Writer = nsqd.NewBufioWriterSize(snappy.NewWriter(conn), int(atomic.LoadInt64(&c.outputBufferSize)))

	atomic.StoreInt32(&c.Snappy, 1)

	//TODO upgrade connection to current nsq

	return nil
}

// WriteCommand is a goroutine safe method to write a Command
// to this connection, and flush.
func (c *SyncClient) WriteCommand(cmd *nsq.Command) error {
	c.writeLock.Lock()

	_, err := cmd.WriteTo(c)
	if err != nil {
		goto exit
	}
	err = c.Flush()

	exit:
	c.writeLock.Unlock()
	if err != nil {
		nsqSyncLog.Errorf("IO error - %s", err)
	}
	return err
}

func (c *SyncClient) GetIdentityData() *nsqd.IdentifyDataV2 {
	c.metaLock.RLock()
	defer c.metaLock.RUnlock()
	return c.identityData
}

func (c *SyncClient) SetIdentityData(newIdentityData *nsqd.IdentifyDataV2) {
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	// TODO: for backwards compatibility, remove in 1.0
	hostname := newIdentityData.Hostname
	if hostname == "" {
		hostname = newIdentityData.LongID
	}
	// TODO: for backwards compatibility, remove in 1.0
	clientID := newIdentityData.ClientID
	if clientID == "" {
		clientID = newIdentityData.ShortID
	}

	c.ClientID = clientID
	c.Hostname = hostname
	c.UserAgent = newIdentityData.UserAgent
	c.identityData = newIdentityData
}

func (c *SyncClient) String() string {
	return c.RemoteAddr().String()
}