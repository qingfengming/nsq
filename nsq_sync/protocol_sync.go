package nsq_sync

import (
	"bytes"
	"github.com/youzan/nsq/internal/protocol"
	"fmt"
	"net"
	"io"
	"encoding/binary"
	"github.com/youzan/nsq/internal/levellogger"
	"errors"
	"time"
	"strings"
	"sync/atomic"
	"encoding/json"
	"github.com/youzan/nsq/nsqd"
	"math"
	"github.com/youzan/nsq/internal/version"
	"strconv"
	"github.com/youzan/go-nsq"
	"sync"
)

const (
	E_INVALID         = "E_INVALID"
	E_TOPIC_NOT_EXIST = "E_TOPIC_NOT_EXIST"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var topicProducerManager *nsq.TopicProducerMgr
var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")
var offsetSplitStr = ":"
var offsetSplitBytes = []byte(offsetSplitStr)
var once sync.Once

var (
	ErrOrderChannelOnSampleRate = errors.New("order consume is not allowed while sample rate is not 0")
	ErrPubToWaitTimeout         = errors.New("pub to wait channel timeout")
)

type protocolSync struct {
	ctx *context
}

func InitProducerManager(opts *Options) error {
	var err error
	once.Do(func () {
		pCfg := nsq.NewConfig()
		topicProducerManager, err = nsq.NewTopicProducerMgr([]string{}, pCfg)
		err = topicProducerManager.ConnectToNSQLookupd(opts.NSQLookupdHttpAddress)
		if err != nil {
			fmt.Printf("fail to initialize topic producer manager, err: %v", err)
			nsqSyncLog.Errorf("fail to initialize topic producer manager, err: %v", err)
		}
	})
	return err
}

//not heart beat as conenction healthy check is delegated to producer manager
//func (p *protocolProxyForward) heartbeatLoopPump(client *ProxyClient, startedChan chan bool, stoppedChan chan bool, heartbeatStopChan chan bool)

func (p *protocolSync) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time
	left := make([]byte, 100)
	tmpLine := make([]byte, 100)
	clientID := p.ctx.nextClientID()
	client := NewSyncClient(clientID, conn, p.ctx.getOpts(), p.ctx.GetTlsConfig())
	client.SetWriteDeadline(zeroTime)

	for {
		if client.GetHeartbeatInterval() > 0 {
			client.SetReadDeadline(time.Now().Add(client.GetHeartbeatInterval() * 3))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
				if strings.Contains(err.Error(), "timeout") {
					// force close conn to wake up conn.write if timeout since
					// the connection may be dead.
					//TODO client.Exit()
				}
			}
			break
		}

		if nsqSyncLog.Level() > levellogger.LOG_DETAIL {
			nsqSyncLog.Logf("PROTOCOL(V2) got client command: %v ", line)
		}
		// handle the compatible for message id.
		// Since the new message id is id+traceid. we can not
		// use \n to check line.
		// REQ, FIN, TOUCH (with message id as param) should be handled.
		// FIN 16BYTES\n
		// REQ 16bytes time\n
		// TOUCH 16bytes\n
		isSpecial := false
		params := make([][]byte, 0)
		if len(line) >= 3 {
			if bytes.Equal(line[:3], []byte("FIN")) ||
				bytes.Equal(line[:3], []byte("REQ")) {
				isSpecial = true
				if len(line) < 21 {
					left = left[:20-len(line)]
					nr := 0
					nr, err = io.ReadFull(client.Reader, left)
					if err != nil {
						nsqSyncLog.LogErrorf("read param err:%v", err)
					}
					line = append(line, left[:nr]...)
					tmpLine = tmpLine[:len(line)]
					copy(tmpLine, line)
					// the readslice will overwrite the slice line,
					// so we should copy it and copy back.
					extra, extraErr := client.Reader.ReadSlice('\n')
					tmpLine = append(tmpLine, extra...)
					line = append(line[:0], tmpLine...)
					if extraErr != nil {
						nsqSyncLog.LogErrorf("read param err:%v", extraErr)
					}
				}
				params = append(params, line[:3])
				if len(line) >= 21 {
					params = append(params, line[4:20])
					// it must be REQ
					if bytes.Equal(line[:3], []byte("REQ")) {
						if len(line) >= 22 {
							params = append(params, line[21:len(line)-1])
						}
					} else {
						params = append(params, line[20:])
					}
				} else {
					params = append(params, []byte(""))
				}

			} else if len(line) >= 5 {
				if bytes.Equal(line[:5], []byte("TOUCH")) {
					isSpecial = true
					if len(line) < 23 {
						left = left[:23-len(line)]
						nr := 0
						nr, err = io.ReadFull(client.Reader, left)
						if err != nil {
							nsqSyncLog.Logf("TOUCH param err:%v", err)
						}
						line = append(line, left[:nr]...)
					}
					params = append(params, line[:5])
					if len(line) >= 23 {
						params = append(params, line[6:22])
					} else {
						params = append(params, []byte(""))
					}
				}
			}
		}
		if p.ctx.getOpts().Verbose || nsqSyncLog.Level() > levellogger.LOG_DETAIL {
			nsqSyncLog.Logf("PROTOCOL(V2) got client command: %v ", line)
		}
		if !isSpecial {
			// trim the '\n'
			line = line[:len(line)-1]
			// optionally trim the '\r'
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			params = bytes.Split(line, separatorBytes)
		}

		if p.ctx.getOpts().Verbose || nsqSyncLog.Level() > levellogger.LOG_DETAIL {
			nsqSyncLog.Logf("PROTOCOL(V2): [%s] %v, %v", client, string(params[0]), params)
		}

		var response []byte
		response, err = p.Exec(client, params)
		err = handleRequestReponseForClient(client, response, err)
		if err != nil {
			nsqSyncLog.Logf("PROTOCOL(V2) handle client command: %v failed", line)
			break
		}
	}

	if err != nil {
		nsqSyncLog.Logf("PROTOCOL(V2): client [%s] exiting ioloop with error: %v", client, err)
	}
	if nsqSyncLog.Level() >= levellogger.LOG_DEBUG {
		nsqSyncLog.LogDebugf("PROTOCOL(V2): client [%s] exiting ioloop", client)
	}
	close(client.ExitChan)
	//p.ctx.nsqd.CleanClientPubStats(client.String(), "tcp")

	if nsqSyncLog.Level() >= levellogger.LOG_DEBUG {
		nsqSyncLog.Logf("msg pump stopped client %v", client)
	}

	//if client.Channel != nil {
	//	client.Channel.RequeueClientMessages(client.ID, client.String())
	//	client.Channel.RemoveClient(client.ID, client.GetDesiredTag())
	//}
	client.FinalClose()

	return err
}

func handleRequestReponseForClient(client *SyncClient, response []byte, err error) error {
	if err != nil {
		ctx := ""

		if childErr, ok := err.(protocol.ChildErr); ok {
			if parentErr := childErr.Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
		}

		nsqSyncLog.LogDebugf("Error response for [%s] - %s - %s",
			client, err, ctx)

		sendErr := Send(client, frameTypeError, []byte(err.Error()))
		if sendErr != nil {
			nsqSyncLog.LogErrorf("Send response error: [%s] - %s%s", client, sendErr, ctx)
			return err
		}

		// errors of type FatalClientErr should forceably close the connection
		if _, ok := err.(*protocol.FatalClientErr); ok {
			return err
		}
		return nil
	}

	if response != nil {
		sendErr := Send(client, frameTypeResponse, response)
		if sendErr != nil {
			err = fmt.Errorf("failed to send response - %s", sendErr)
		}
	}

	return err
}

/**
TODO: AUTH handle authentication request from peer nsqproxy, and use auth server in configuration for authentication.
Authentication server configured need to be the same one configured in forward nsqd.
 */


func (p *protocolSync) Exec(client *SyncClient, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}
	//err := enforceTLSPolicy(client, p, params[0])
	//if err != nil {
	//	return nil, err
	//}
	switch {
	//case bytes.Equal(params[0], []byte("FIN")):
	//	return p.FIN(client, params)
	//case bytes.Equal(params[0], []byte("RDY")):
	//	return p.RDY(client, params)
	//case bytes.Equal(params[0], []byte("REQ")):
	//	return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	//case bytes.Equal(params[0], []byte("PUB_TRACE")):
	//	return p.PUBTRACE(client, params)
	//case bytes.Equal(params[0], []byte("PUB_EXT")):
	//	return p.PUBEXT(client, params)
	//case bytes.Equal(params[0], []byte("MPUB")):
	//	return p.MPUB(client, params)
	//case bytes.Equal(params[0], []byte("MPUB_TRACE")):
	//	return p.MPUBTRACE(client, params)
	//case bytes.Equal(params[0], []byte("NOP")):
	//	return p.NOP(client, params)
	//case bytes.Equal(params[0], []byte("TOUCH")):
	//	return p.TOUCH(client, params)
	//case bytes.Equal(params[0], []byte("SUB_ADVANCED")):
	//	return p.SUBADVANCED(client, params)
	//case bytes.Equal(params[0], []byte("SUB_ORDERED")):
	//	return p.SUBORDERED(client, params)
	//case bytes.Equal(params[0], []byte("CLS")):
	//	return p.CLS(client, params)
	//case bytes.Equal(params[0], []byte("AUTH")):
	//	return p.AUTH(client, params)
	//case bytes.Equal(params[0], []byte("INTERNAL_CREATE_TOPIC")):
	//	return p.internalCreateTopic(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, E_INVALID, fmt.Sprintf("invalid command %v", params))
}

func (p *protocolSync) PUB(client *SyncClient, params [][]byte) ([]byte, error) {
	return p.internalPubExtAndTrace(client, params, false, false)
}

func (p *protocolSync) internalPubExtAndTrace(client *SyncClient, params [][]byte, pubExt bool, traceEnable bool) ([]byte, error) {
	//startPub := time.Now().UnixNano()
	bodyLen, topic, err := p.preparePub(client, params, p.ctx.getOpts().MaxMsgSize, false)
	if err != nil {
		return nil, err
	}

	if traceEnable && bodyLen <= nsqd.MsgTraceIDLength {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d with trace id enabled", bodyLen))
	}

	if pubExt && bodyLen <= nsqd.MsgJsonHeaderLength {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d with ext json header enabled", bodyLen))
	}

	messageBodyBuffer := topic.BufferPoolGet(int(bodyLen))
	defer topic.BufferPoolPut(messageBodyBuffer)

	topicName := topic.topicName
	_, err = io.CopyN(messageBodyBuffer, client.Reader, int64(bodyLen))
	if err != nil {
		nsqd.NsqLogger().Logf("topic: %v message body read error %v ", topicName, err.Error())
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "failed to read message body")
	}
	messageBody := messageBodyBuffer.Bytes()[:bodyLen]

	var realBody []byte
	//TODO: other flag: pubTrace & ext
	realBody = messageBody
	fmt.Printf("pub to topic: %v, %v, msg: %v", topic.topicName, topic.partition, string(realBody))
	//publish to topic manager
	//err = topicProducerManager.Publish(topic.topicName, realBody)
	err = topicProducerManager.PublishWithPartition(topic.topicName, topic.partition, realBody)
	if err != nil {
		return nil, protocol.NewClientErr(err, err.Error(), "")
	}
	return okBytes, nil
}

func isTopicProducerNotFound(err error) bool {
	return strings.Contains(err.Error(), "topicProducer not found")
}

//if target topic is not configured as extendable and there is a tag, pub request should be stopped here
func (p *protocolSync) preparePub(client *SyncClient, params [][]byte, maxBody int64, isMpub bool) (int32, *TopicInfo, error) {
	var err error

	if len(params) < 2 {
		return 0, nil, protocol.NewFatalClientErr(nil, E_INVALID, "insufficient number of parameters")
	}

	topicName := string(params[1])
	partition := -1
	if len(params) == 3 {
		partition, err = strconv.Atoi(string(params[2]))
		if err != nil {
			return 0, nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
				fmt.Sprintf("topic partition is not valid: %v", err))
		}
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return 0, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "failed to read body size")
	}

	if bodyLen <= 0 {
		return bodyLen, nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > maxBody {
		nsqd.NsqLogger().Logf("topic: %v message body too large %v vs %v ", topicName, bodyLen, maxBody)
		if isMpub {
			return bodyLen, nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
				fmt.Sprintf("body too big %d > %d", bodyLen, maxBody))
		} else {
			return bodyLen, nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("message too big %d > %d", bodyLen, maxBody))
		}
	}

	//TODO:
	//if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
	//	return bodyLen, nil, err
	//}
	topic := p.ctx.nsqsync.getTopic(topicName, partition)
	return bodyLen, topic, nil
}


func (p *protocolSync) IDENTIFY(client *SyncClient, params [][]byte) ([]byte, error) {
	var err error
	state := atomic.LoadInt32(&client.State)
	if state != stateInit {
		nsqSyncLog.LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.ctx.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ctx.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData nsqd.IdentifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): [%s] %+v", client, identifyData)

	//update identify data to proxy client, update consumer, if consumer is connected
	err = client.Identify(&identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}
	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.ctx.GetTlsConfig() != nil && identifyData.TLSv1
	deflate := p.ctx.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 0
	if deflate {
		if identifyData.DeflateLevel <= 0 {
			deflateLevel = 6
		}
		deflateLevel = int(math.Min(float64(identifyData.DeflateLevel), float64(p.ctx.getOpts().MaxDeflateLevel)))
	}
	snappy := p.ctx.getOpts().SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
		DesiredTag          string `json:"desired_tag,omitempty"`
	}{
		MaxRdyCount:         p.ctx.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.ctx.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(p.ctx.getOpts().MaxMsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.ctx.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          0,
		AuthRequired:        false,
		OutputBufferSize:    int(client.GetOutputBufferSize()),
		OutputBufferTimeout: int64(client.GetOutputBufferTimeout() / time.Millisecond),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	//upgrade connections to backword nsq client
	//TODO ignore tlsv1 :
	//if tlsv1 {
	//	nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to TLS", client)
	//	err = client.UpgradeTLS()
	//	if err != nil {
	//		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	//	}
	//
	//	err = Send(client, frameTypeResponse, okBytes)
	//	if err != nil {
	//		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	//	}
	//}
	//
	//if snappy {
	//	nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to snappy", client)
	//	err = client.UpgradeSnappy()
	//	if err != nil {
	//		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	//	}
	//
	//	err = Send(client, frameTypeResponse, okBytes)
	//	if err != nil {
	//		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	//	}
	//}
	//
	//if deflate {
	//	nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to deflate (level %d)", client, deflateLevel)
	//	err = client.UpgradeDeflate(deflateLevel)
	//	if err != nil {
	//		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	//	}
	//
	//	err = Send(client, frameTypeResponse, okBytes)
	//	if err != nil {
	//		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	//	}
	//}

	return nil, nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func Send(client *SyncClient, frameType int32, data []byte) error {
	return internalSend(client, frameType, data, false)
}

func internalSend(client *SyncClient, frameType int32, data []byte, needFlush bool) error {
	client.writeLock.Lock()
	defer client.writeLock.Unlock()
	if client.Writer == nil {
		return errors.New("client closed")
	}

	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		return err
	}

	if needFlush || frameType != frameTypeMessage {
		err = client.Flush()
	}
	return err
}