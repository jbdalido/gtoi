package main

import (
	"encoding/json"
	"fmt"
	"github.com/jbdalido/gtoi/Godeps/_workspace/src/github.com/influxdb/influxdb/client"
	log "github.com/jbdalido/gtoi/Godeps/_workspace/src/github.com/jbdalido/logrus"
)

type InfluxDBClient struct {
	Client *client.Client
}

func NewInfluxClient(host, user, pass, db string) (*InfluxDBClient, error) {
	log.Printf("Host: %s  User : %s Pass : %s DB : %s", host, user, pass, db)
	c, err := client.NewClient(&client.ClientConfig{
		Host:     host,
		Username: user,
		Password: pass,
		Database: db,
	})

	if err != nil {
		return nil, fmt.Errorf("InfluxDB Connection ERROR : %s ", err)
	}

	return &InfluxDBClient{
		Client: c,
	}, nil
}

func (i *InfluxDBClient) WriteSeries(series []*client.Series) error {
	err := i.Client.WriteSeries(series)
	if err != nil {
		errorLog, er := json.Marshal(series)
		if er != nil {
			log.Errorf("Cant unmarshal data no error log")
		}
		log.Errorf("Cant write series %s", err)
		log.Errorf(string(errorLog))

		return err
	}
	return nil
}
