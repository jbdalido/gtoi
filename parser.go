package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jbdalido/gtoi/Godeps/_workspace/src/github.com/influxdb/influxdb/client"
	log "github.com/jbdalido/gtoi/Godeps/_workspace/src/github.com/jbdalido/logrus"
	"github.com/jbdalido/gtoi/Godeps/_workspace/src/gopkg.in/yaml.v2"
	"os/user"
	"strconv"
	"strings"
)

type Parser struct {
	Config   string
	Patterns map[string]Pattern `yaml:"patterns"`
}

type Pattern struct {
	Pattern string         `yaml:"pattern"`
	Columns map[string]int `yaml:"columns"`
}

func NewParser(config string) (*Parser, error) {
	p := &Parser{
		Config: config,
	}

	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	if strings.Contains(p.Config, "~") {
		p.Config = strings.Replace(config, "~", usr.HomeDir, 1)
	}

	datas, err := OpenAndReadFile(p.Config)
	if err == nil {
		if err := yaml.Unmarshal(datas, p); err != nil {
			return nil, fmt.Errorf("Error processing %s: %s", p.Config, err)
		}
	} else {
		log.Errorf("Config file not found, ignoring ...")
	}

	return p, nil
}

func (p *Parser) ParseChunck(data []byte) ([]*client.Series, error) {
	readbuffer := bytes.NewBuffer(data)
	r := bufio.NewReader(readbuffer)
	var series []*client.Series
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			break
		}
		s, err := p.ParseLine(line)
		if err != nil {
			log.Error(err)
			continue
		}
		series = append(series, s)
	}
	errorLog, err := json.Marshal(series)
	if err != nil {
		log.Errorf("Cant unmarshal data no error log")
	}
	log.Printf("Sending %s", errorLog)
	return series, nil
}

// ParseLine returns an array of client Serie
// 0 element is host
// last element is type
// value comes after first space
// times comes after last space
// If we have a match, then first element before space is exploded by .
// and name of columns are set
func (p *Parser) ParseLine(line []byte) (*client.Series, error) {
	log.Infof("Parsing line : %s", line)
	l := string(line)

	columns := strings.Split(l, "\t")
	if len(columns) != 3 {
		return nil, fmt.Errorf("invalid line format %d", len(columns))
	}
	// Unmodified values
	name := columns[0]
	value := columns[1]
	time, _ := strconv.ParseFloat(columns[2], 64)
	// Explode the serie to find relevant values
	precise := strings.Split(name, ".")
	hostname := precise[0]
	// Here setup regexp matching
	// if
	//  blablabla
	// endif
	serie := &client.Series{
		Name:    name,
		Columns: []string{"time", "hostname", "value"},
		Points: [][]interface{}{
			{time, hostname, value},
		},
	}
	return serie, nil

}
