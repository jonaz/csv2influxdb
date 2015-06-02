package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	"github.com/influxdb/influxdb/client"
)

func main() {
	app := cli.NewApp()
	app.Name = "greet"
	app.Usage = "fight the loneliness!"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "database",
			Value: "",
			Usage: "the database",
		},
		cli.StringFlag{
			Name:  "server",
			Value: "",
			Usage: "the server",
		},
		cli.StringFlag{
			Name:  "filename",
			Value: "",
			Usage: "filename to read",
		},
		cli.BoolFlag{
			Name:  "includefirstline",
			Usage: "include first line in csv file",
		},
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "verbose",
		},
	}
	app.Action = csv2influxdb
	app.Run(os.Args)
}

func csv2influxdb(c *cli.Context) {
	csvfile, err := os.Open(c.String("filename"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvfile.Close()
	reader := csv.NewReader(csvfile)
	reader.Comma = ';'

	count, err := lineCounter(c.String("filename"))
	if err != nil {
		fmt.Println(err)
		return
	}

	var pts = make([]client.Point, count)
	i := 0
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}

		if i == 0 && !c.Bool("includefirstline") {
			i++
			continue
		}

		if data[2] == "NaN" {
			data[2] = "0"
		}

		data[2] = strings.Replace(data[2], ",", ".", -1)
		temp, _ := strconv.ParseFloat(data[2], 64)

		if c.Bool("verbose") {
			log.Println("time: " + data[0] + " " + data[1] + "\t temp: " + data[2])
		}
		time, err := time.Parse("2006-01-02 15:04:05", data[0]+" "+data[1])
		if err != nil {
			fmt.Println(err)
			return
		}

		pts[i] = client.Point{
			Measurement: "outsideTemp",
			Fields: map[string]interface{}{
				"value": temp,
			},
			Time: time,
		}

		i++
	}
	influx := &InfluxDb{}
	err = influx.Connect(c.String("server"))
	if err != nil {
		log.Println(err)
		return
	}

	influx.Log(c.String("database"), pts)
}

func lineCounter(filename string) (int, error) {
	r, err := os.Open(filename)
	if err != nil {
		return 0, err
	}

	defer r.Close()
	buf := make([]byte, 8196)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return count, err
		}

		count += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return count, nil
}
