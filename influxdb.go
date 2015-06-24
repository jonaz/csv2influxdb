package main

import (
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/influxdb/influxdb/client"
	serverprotocol "github.com/stampzilla/stampzilla-go/nodes/stampzilla-server/protocol"
)

type InfluxDb struct {
	Nodes *serverprotocol.Nodes `inject:""`
	conn  *client.Client
}

func NewInfluxDb() *InfluxDb {
	return &InfluxDb{}
}

func (i *InfluxDb) Connect(server string) error {

	u, err := url.Parse(fmt.Sprintf("http://%s:8086", server))
	if err != nil {
		return err
	}

	conf := client.Config{
		URL:      *u,
		Username: os.Getenv("INFLUX_USER"),
		Password: os.Getenv("INFLUX_PWD"),
	}

	i.conn, err = client.NewClient(conf)
	if err != nil {
		return err
	}

	dur, ver, err := i.conn.Ping()
	if err != nil {
		return err
	}
	log.Println("Connected to influxdb: %v, %s", dur, ver)

	return nil
}

func (self *InfluxDb) Log(db string, pts []client.Point) {
	bp := client.BatchPoints{
		Points:   pts,
		Database: db,
	}
	//b, _ := json.Marshal(&bp)
	//fmt.Println(string(b))
	//return

	_, err := self.conn.Write(bp)
	if err != nil {
		log.Println(err)
	}
}
