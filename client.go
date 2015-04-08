// A Riemann client for Go, featuring concurrency, sending events and state updates, queries,
// and feature parity with the reference implementation written in Ruby.
//
// Copyright (C) 2014 by Christopher Gilbert <christopher.john.gilbert@gmail.com>
package goryman

import (
	"net"
	"time"

	"github.com/bigdatadev/goryman/proto"
)

// GorymanClient is a client library to send events to Riemann
type GorymanClient struct {
	tcp  *Transport
	addr string
}

// NewGorymanClient - Factory
func NewGorymanClient(addr string) *GorymanClient {
	return &GorymanClient{
		addr: addr,
	}
}

// Connect creates a UDP and TCP connection to a Riemann server
func (c *GorymanClient) Connect() error {
	tcp, err := net.DialTimeout("tcp", c.addr, time.Second*5)
	if err != nil {
		return err
	}
	c.tcp = NewTcpTransport(tcp)
	return nil
}

// Close the connection to Riemann
func (c *GorymanClient) Close() error {
	return c.tcp.Close()
}

// Send an event
func (c *GorymanClient) SendEvent(e *Event) error {
	epb, err := EventToProtocolBuffer(e)
	if err != nil {
		return err
	}

	message := &proto.Msg{}
	message.Events = append(message.Events, epb)

	return c.tcp.Send(message)
}

// Send a state update
func (c *GorymanClient) SendState(s *State) error {
	spb, err := StateToProtocolBuffer(s)
	if err != nil {
		return err
	}

	message := &proto.Msg{}
	message.States = append(message.States, spb)

	return c.tcp.Send(message)
}
