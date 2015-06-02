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

	con, err := client.NewClient(conf)
	if err != nil {
		return err
	}

	dur, ver, err := con.Ping()
	if err != nil {
		return err
	}
	log.Println("Connected to influxdb: %v, %s", dur, ver)

	return nil
}

func (self *InfluxDb) Log(db string, pts []client.Point) {
	bps := client.BatchPoints{
		Points:   pts,
		Database: db,
	}
	_, err := self.conn.Write(bps)
	if err != nil {
		log.Println(err)
	}
}
func (self *InfluxDb) Commit(s interface{}) {
}
