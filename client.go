package main

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/GaryBoone/GoStats/stats"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Client struct {
	ID         int
	BrokerURL  string
	BrokerUser string
	BrokerPass string
	MsgTopic   string
	MsgSize    int
	MsgCount   int
	MsgRate    int
	MsgQoS     byte
	Quiet      bool
}

func (c *Client) Run(res chan *RunResults) {
	newMsgs := make(chan *Message)
	pubMsgs := make(chan *Message)
	doneGen := make(chan bool)
	donePub := make(chan bool)
	runResults := new(RunResults)

	started := time.Now()
	// start generator
	go c.genMessages(newMsgs, doneGen)
	// start publisher
	go c.pubMessages(newMsgs, pubMsgs, doneGen, donePub)

	runResults.ID = c.ID
	times := []float64{}
	for {
		select {
		case m := <-pubMsgs:
			if m.Error {
				log.Printf("CLIENT %v ERROR publishing message: %v: at %v\n", c.ID, m.Topic, m.Sent.Unix())
				runResults.Failures++
			} else {
				// log.Printf("Message published: %v: sent: %v delivered: %v flight time: %v\n", m.Topic, m.Sent, m.Delivered, m.Delivered.Sub(m.Sent))
				runResults.Successes++
				times = append(times, m.Delivered.Sub(m.Sent).Seconds()*1000) // in milliseconds
			}
		case <-donePub:
			// calculate results
			duration := time.Now().Sub(started)
			runResults.MsgTimeMin = stats.StatsMin(times)
			runResults.MsgTimeMax = stats.StatsMax(times)
			runResults.MsgTimeMean = stats.StatsMean(times)
			runResults.MsgTimeStd = stats.StatsSampleStandardDeviation(times)
			runResults.RunTime = duration.Seconds()
			runResults.MsgsPerSec = float64(runResults.Successes) / duration.Seconds()

			// report results and exit
			res <- runResults
			return
		}
	}
}

func (c *Client) genMessages(ch chan *Message, done chan bool) {

	tick := int64(0)
	if c.MsgRate > 0 {
		tick = int64(time.Second) / int64(c.MsgRate)
		log.Printf("Tick for %v nanoseconds\n", tick)
	}
	start := time.Now().UnixNano()
	topic := fmt.Sprintf("%v/%v/0", c.MsgTopic, c.ID)
	payload := make([]byte, c.MsgSize)
	payloadBuf := bytes.NewBuffer(payload)

	for i := 0; i < c.MsgCount; i++ {
		payloadBuf.Reset()
		payloadBuf.WriteString(c.MsgTopic)
		payloadBuf.WriteString(",")
		payloadBuf.WriteString(strconv.Itoa(c.ID))
		payloadBuf.WriteString(",0,")
		payloadBuf.WriteString(strconv.Itoa(i))
		ch <- &Message{
			Topic:   topic,
			QoS:     c.MsgQoS,
			Payload: payloadBuf.String(),
		}

		if tick > 0 {
			for delay := (start + tick - time.Now().UnixNano()); delay > 0; {
				time.Sleep(time.Duration(delay))
				delay = (start + tick - time.Now().UnixNano())
			}
			start += tick
		}
	}
	done <- true
	// log.Printf("CLIENT %v is done generating messages\n", c.ID)
	return
}

func (c *Client) pubMessages(in, out chan *Message, doneGen, donePub chan bool) {
	onConnected := func(client mqtt.Client) {
		if !c.Quiet {
			log.Printf("CLIENT %v is connected to the broker %v\n", c.ID, c.BrokerURL)
		}
		ctr := 0
		for {
			select {
			case m := <-in:
				m.Sent = time.Now()
				//log.Printf("CLIENT %v publish on %v : %v\n", c.ID, m.Topic, m.Payload)
				token := client.Publish(m.Topic, m.QoS, false, m.Payload)
				token.Wait()
				if token.Error() != nil {
					log.Printf("CLIENT %v Error sending message: %v\n", c.ID, token.Error())
					m.Error = true
				} else {
					m.Delivered = time.Now()
					m.Error = false
				}
				out <- m

				if ctr > 0 && ctr%100 == 0 {
					if !c.Quiet {
						log.Printf("CLIENT %v published %v messages and keeps publishing...\n", c.ID, ctr)
					}
				}
				ctr++
			case <-doneGen:
				donePub <- true
				if !c.Quiet {
					log.Printf("CLIENT %v is done publishing\n", c.ID)
				}
				return
			}
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now(), c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(onConnected).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("CLIENT %v lost connection to the broker: %v. Will reconnect...\n", c.ID, reason.Error())
		})
	if c.BrokerUser != "" && c.BrokerPass != "" {
		opts.SetUsername(c.BrokerUser)
		opts.SetPassword(c.BrokerPass)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()

	if token.Error() != nil {
		log.Printf("CLIENT %v had error connecting to the broker: %v\n", c.ID, token.Error())
	}
}
