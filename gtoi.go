package main

import (
	"os"

	"github.com/jbdalido/gtoi/Godeps/_workspace/src/github.com/codegangsta/cli"
	log "github.com/jbdalido/gtoi/Godeps/_workspace/src/github.com/jbdalido/logrus"
)

func main() {
	// New cliapp
	cliApp := cli.App{
		Name:    "gtoi",
		Usage:   "Convert graphite formatted datas to column in influxdb",
		Version: "0.0.1",
	}
	// Send Flags
	sendFlags := []cli.Flag{
		cli.StringFlag{
			Name:   "host, H",
			Value:  "127.0.0.1:9666",
			Usage:  "Influxdb endpoint",
			EnvVar: "GTOI_HOST",
		},
	}
	// Setup customs flags
	startFlags := []cli.Flag{
		cli.StringFlag{
			Name:   "host, H",
			Value:  "127.0.0.1:8086",
			Usage:  "Influxdb endpoint",
			EnvVar: "GTOI_HOST",
		},
		cli.StringFlag{
			Name:   "user, u",
			Value:  "root",
			Usage:  "Influxdb user",
			EnvVar: "GTOI_USER",
		},
		cli.StringFlag{
			Name:   "password, p",
			Value:  "root",
			Usage:  "Influxdb password",
			EnvVar: "GTOI_PASSWORD",
		},
		cli.StringFlag{
			Name:   "db, d",
			Value:  "testing",
			Usage:  "Influxdb DB",
			EnvVar: "GTOI_DB",
		},
		cli.StringFlag{
			Name:   "listen, l",
			Value:  "0.0.0.0:9666",
			Usage:  "Listening interface and port",
			EnvVar: "GTOI_LISTEN",
		},
		cli.StringFlag{
			Name:   "config, c",
			Value:  "/etc/gtoi/gtoi.yml",
			Usage:  "Pattern config file",
			EnvVar: "GTOI_PATTERNS",
		},
		cli.IntFlag{
			Name:   "workers, w",
			Value:  5,
			Usage:  "Number of connections to InfluxDB",
			EnvVar: "GTOI_WORKERS",
		},
		cli.IntFlag{
			Name:   "pool",
			Value:  5000,
			Usage:  "TCP pool size",
			EnvVar: "GTOI_POOL",
		},
	}
	cliApp.Commands = []cli.Command{
		cli.Command{
			Name:   "start",
			Usage:  "Start daemon",
			Action: Start,
			Flags:  startFlags,
		},
		cli.Command{
			Name:   "send",
			Usage:  "pipe things, i'll send them",
			Action: Pipe,
			Flags:  sendFlags,
		},
	}
	if len(os.Args) == 1 {
		os.Args = append(os.Args, "help")
	}

	// Run gtoi
	cliApp.Run(os.Args)

}

func Start(c *cli.Context) {
	// Get Cli parameters
	config := &ServerConfig{
		Host:     c.String("host"),
		User:     c.String("user"),
		Pass:     c.String("password"),
		DB:       c.String("db"),
		Listen:   c.String("listen"),
		Patterns: c.String("config"),
		Workers:  c.Int("workers"),
		Pool:     c.Int("pool"),
	}
	log.Infof("Starting GTOI ...")
	// Start the server
	server, err := NewServer(config)
	if err != nil {
		log.Fatal(err)
	}
	err = server.Start()
	if err != nil {
		log.Fatal(err)
	}
}

func Pipe(c *cli.Context) {
	err := Send(c.String("host"))
	if err != nil {
		log.Fatal(err)
	}
}
