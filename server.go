package main

import (
	"fmt"
	log "github.com/jbdalido/gtoi/Godeps/_workspace/src/github.com/jbdalido/logrus"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
)

const (
	PROTOCOL = "tcp"
)

type Server struct {
	Listen   string
	Received chan []byte
	Workers  chan Worker
}

type ServerConfig struct {
	Host     string
	User     string
	Pass     string
	Patterns string
	DB       string
	Listen   string
	Workers  int
	Pool     int
}

func NewServer(c *ServerConfig) (*Server, error) {
	// Setup basic server configuration
	log.Infof("Setting up Pool with %d elements", c.Pool)
	server := &Server{
		Listen:   c.Listen,
		Workers:  make(chan Worker, c.Pool),
		Received: make(chan []byte, 2048),
	}
	//Setup Parser
	p, err := NewParser(c.Patterns)
	if err != nil {
		return nil, err
	}
	// Setup Workers
	nbWorkers := 0
	if c.Workers >= 0 {
		nbWorkers = c.Workers
	}
	for i := 0; i < nbWorkers; i++ {
		log.Infof("Setting up Worker #%d", i)
		influxClient, err := NewInfluxClient(c.Host, c.User, c.Pass, c.DB)
		if err != nil {
			return nil, fmt.Errorf("InfluxDB Error : %s", err)
		}
		w := Worker{
			ID:     i,
			Parser: p,
			Influx: influxClient,
		}
		server.Workers <- w
	}

	return server, nil

}

func Send(host string) error {
	conn, err := net.Dial(PROTOCOL, host)
	if err != nil {
		return err
		os.Exit(1)
	}
	bytes, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return err
		os.Exit(1)
	}
	conn.Write(bytes)
	conn.Close()

	return nil

}

func (s *Server) Start() error {
	// Setup error channel
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	werr := make(chan error, 1)
	aerr := make(chan error, 1)
	// Start Workers
	go func() {
		log.Infof("Starting Workers ...")
		werr = s.StartWorkers()
	}()

	// Start TCP Listener
	go func() {
		log.Printf("Starting Listener ...")
		aerr = s.StartListen()
	}()

	// Wait for it to crash
	for {
		select {
		case err := <-werr:
			log.Fatalf("WORKER ERROR %s", err)
		case err := <-aerr:
			log.Fatalf("TCP ERROR %s", err)
		case <-stop:
			log.Printf("Stopping ...")
			os.Exit(0)
		}
	}
}

func (s *Server) StartWorkers() chan error {
	for {
		select {
		case data := <-s.Received:
			//log.Infof("Waiting for an available worker ...")
			w := <-s.Workers
			go func() {
				//log.Infof("Worker#%d available ...", w.ID)
				err := w.Insert(data)
				if err != nil {
					log.Warningf("Insert Error %s ", w.ID, err)
				}
				log.Infof("Releasing worker#%d ...", w.ID)
				s.Workers <- w
			}()
		}
	}
}

func (s *Server) StartListen() chan error {
	server, err := net.Listen(PROTOCOL, s.Listen)
	if err != nil {
		log.Fatalf("%s", err)
	}
	defer server.Close()
	for {
		conn, err := server.Accept()
		if err != nil {
			log.Errorf("Socket Error : %s ", err)
		}
		go s.ReadConn(conn)
	}
}

func (s *Server) ReadConn(conn net.Conn) error {
	buf, err := ioutil.ReadAll(conn)
	if err != nil {
		return err
	}
	conn.Write([]byte("ok"))
	conn.Close()
	s.Received <- buf

	return err
}

type Worker struct {
	ID     int
	Influx *InfluxDBClient
	Parser *Parser
	Done   int64
	Error  int64
}

func (w *Worker) Insert(data []byte) error {
	//log.Printf("\n%s", data)
	series, err := w.Parser.ParseChunck(data)
	if err != nil {
		log.Errorf("%s", err)
		return err
	}
	w.Influx.WriteSeries(series)
	return nil
}
