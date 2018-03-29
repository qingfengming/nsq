package main

import (
	"os"
	"fmt"
	"flag"
	"log"
	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"time"
	"github.com/absolute8511/glog"
	"github.com/youzan/nsq/internal/version"
	"github.com/youzan/nsq/nsq_sync"
	"github.com/judwhite/go-svc/svc"
	"path/filepath"
)

func nsqsyncFlagSet(opts *nsq_sync.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqsync", flag.ExitOnError)

	flagSet.String("config", "", "path to config file")
	flagSet.Bool("version", false, "print version string")

	flagSet.String("http-address", "0.0.0.0:4160", "<addr>:<port> to listen on for HTTP access")
	flagSet.String("http-address", "0.0.0.0:4150", "<addr>:<port> to listen on for peer nsq sync")
	flagSet.String("peer-nsq-sync-http", "", "<addr>:<port> peer nsq sync HTTP address to access")
	flagSet.String("peer-nsq-sync-tcp", "", "<addr>:<port> peer nsq sync TCP address to access")
	flagSet.String("nsqlookupd-http-address", "", "<addr>:<port> nsqlookupd http address backwards")

	flagSet.Duration("max-msg-timeout", opts.MaxMsgTimeout, "maximum duration before a message will timeout")
	flagSet.Int64("max-msg-size", opts.MaxMsgSize, "maximum size of a single message in bytes")
	flagSet.Int64("max-rdy-count", opts.MaxRdyCount, "maximum RDY count for a client")
	// remove, deprecated
	flagSet.Int64("max-message-size", opts.MaxMsgSize, "(deprecated use --max-msg-size) maximum size of a single message in bytes")
	flagSet.Int64("max-body-size", opts.MaxBodySize, "maximum size of a single command body")

	flagSet.String("template-dir", "", "path to templates directory")
	flagSet.String("log-dir", "", "directory for logs")
	return flagSet
}

type config map[string]interface{}

type program struct {
	nsqSync *nsq_sync.NsqSync
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() {
	opts := nsq_sync.NewOptions()

	flagSet := nsqsyncFlagSet(opts)
	glog.InitWithFlag(flagSet)

	flagSet.Parse(os.Args[1:])
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqsync"))
		os.Exit(0)
	}

	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", configFile, err.Error())
		}
	}

	options.Resolve(opts, flagSet, cfg)
	if opts.LogDir != "" {
		glog.SetGLogDir(opts.LogDir)
	}
	glog.StartWorker(time.Second * 2)
	nsq_sync.SetLogger(opts.Logger, opts.LogLevel)

	daemon := nsq_sync.New(opts)
	daemon.Main()
	p.nsqSync = daemon
	return nil
}

func (p *program) Stop() error {
	if p.nsqSync != nil {
		p.nsqSync.Exit()
	}
	return nil
}