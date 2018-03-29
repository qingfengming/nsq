package nsq_sync

import (
	"github.com/youzan/nsq/internal/levellogger"
	"time"
)

type Options struct {
	Verbose bool `flag:"verbose"`
	//peer nsqproxy http/tcp port and lookup http port
	PeerNSQSyncHTTPAddr string `flag:"peer-nsq-sync-http"`
	PeerNSQSyncTCPAddr string `flag:"peer-nsq-sync-tcp"`
	TCPAddress string `flag:"tcp-address"`
	HTTPAddress string `flag:"http-address"`
	NSQLookupdHttpAddress string `flag:"nsqlookupd-http-address"`

	ConnTimeout	  time.Duration `flag:"conn-timeout"`
	ReadTimeout	  time.Duration `flag:"read-timeout"`
	WriteTimeout	  time.Duration `flag:"write-timeout"`

	MaxBodySize       int64         `flag:"max-body-size"`
	MaxRdyCount            int64         `flag:"max-rdy-count"`
	MaxMsgTimeout     time.Duration `flag:"max-msg-timeout"`
	MsgTimeout        time.Duration `flag:"msg-timeout" arg:"60s"`
	MaxMsgSize        int64         `flag:"max-msg-size" deprecated:"max-message-size" cfg:"max_msg_size"`

	//TODO: compression
	DeflateEnabled  bool `flag:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy"`

	// TLS config
	TLSCert             string `flag:"tls-cert"`
	TLSKey              string `flag:"tls-key"`
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"`
	TLSRootCAFile       string `flag:"tls-root-ca-file"`
	TLSRequired         int    `flag:"tls-required"`
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	LogLevel int32  `flag:"log-level" cfg:"log_level"`
	LogDir   string `flag:"log-dir" cfg:"log_dir"`
	Logger   levellogger.Logger
}

func NewOptions() *Options {
	return &Options{
		HTTPAddress:	    "0.0.0.0:4160",
		TCPAddress:       "0.0.0.0:4150",

		LogLevel: levellogger.LOG_INFO,
		LogDir:   "",
		Logger:   &levellogger.GLogger{},

		MaxBodySize:       5 * 1024 * 1024,
		ConnTimeout: 60*time.Second,
		ReadTimeout: 60*time.Second,
		WriteTimeout: 60*time.Second,

		MaxRdyCount: 2500,
		MaxMsgSize:        1024 * 1024,
		//TODO: compression settings
		//DeflateEnabled:  true,
		//MaxDeflateLevel: 6,
		//SnappyEnabled:   true,
	}
}