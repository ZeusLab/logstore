package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"time"
)

type WsConnection struct {
	ws   *websocket.Conn
	send chan []byte
}

func (c *WsConnection) read() {
	defer func() {
		_ = c.ws.Close()
	}()
	pongWait := 30 * time.Second
	c.ws.SetReadLimit(1048576)
	err := c.ws.SetReadDeadline(time.Now().Add(pongWait))
	if err != nil {
		log.Println(err)
		return
	}
	c.ws.SetPongHandler(func(string) error {
		err = c.ws.SetReadDeadline(time.Now().Add(pongWait));
		if err != nil {
			log.Println(err)
		}
		return err
	})
	for {
		msgType, msg, err := c.ws.ReadMessage()
		if err != nil {
			log.Println(err)
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Printf("error: %v", err)
			}
			break
		}
		log.Printf("receive from %s msg with type = %s and payload = %s\n", c.ws.RemoteAddr(), msgType, string(msg))
		fmt.Printf("sent back to %s a msg: %s\n", c.ws.RemoteAddr(), string(msg))

		// Write message back to browser
		c.send <- msg
		//process message
		//send back tp c.send
	}
}

func (c *WsConnection) write() {
	defer func() {
		close(c.send)
	}()
	for {
		bytes := <-c.send
		_ = c.ws.WriteMessage(websocket.TextMessage, bytes)
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func webSocket(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	conn, err := upgrader.Upgrade(w, r, nil) // error ignored for sake of simplicity
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("accept connection from %v\n", conn.RemoteAddr())
	wsc := &WsConnection{
		ws:   conn,
		send: make(chan []byte, 256),
	}
	defer func() {
		_ = wsc.ws.Close()
		close(wsc.send)
	}()
	go wsc.write()
	wsc.read()
}
