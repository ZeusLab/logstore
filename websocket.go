package main

import (
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
	"hermes/core"
	"log"
	"math"
	"net/http"
	"time"
)

type WsConnection struct {
	ws   *websocket.Conn
	send chan []byte
}

type WSData struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

type LogQuery struct {
	Tag        string `json:"tag"`
	LogLevel   int32  `json:"level"`
	TimeOption `json:"time"`
}

type TimeOption struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

const (
	TopicPing  = "ping"
	TopicQuery = "query"
)

func (c *WsConnection) read(ctx context.Context) {
	c.ws.SetReadLimit(32 * 1024)
	for {
		err := c.ws.SetReadDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			log.Println(err)
			return
		}
		msgType, msg, err := c.ws.ReadMessage()
		if err != nil {
			log.Println(err)
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Printf("error: %v", err)
			}
			break
		}
		if msgType != websocket.TextMessage {
			log.Printf("receive unexpected type of message %d\n", msgType)
			continue
		}
		var wsData WSData
		err = json.Unmarshal(msg, &wsData)
		if err != nil {
			log.Printf("can not unmarshal data from client %v. Discard connection\n", err)
			break
		}
		log.Printf("receive command '%s' of topic '%s' from client %s\n", wsData.Data, wsData.Topic, c.ws.RemoteAddr())
		switch wsData.Topic {
		case TopicPing:
			log.Printf("sent pong to %s\n", c.ws.RemoteAddr())
			c.send <- msg
			break
		case TopicQuery:
			var query LogQuery
			err = json.Unmarshal([]byte(wsData.Data), &query)
			if err != nil {
				log.Printf("Can not unmarshal query from client %s\n", c.ws.RemoteAddr())
				break
			}
			if core.StrIsEmpty(query.Tag) {
				log.Println("missing tag in query option")
				break
			}

			response := make(chan core.OutputLogMessage)
			go func(c context.Context, response chan core.OutputLogMessage, send chan []byte) {
				exit := false
				for !exit {
					select {
					case <-c.Done():
						break
					case msg := <-response:
						if msg.Code != http.StatusOK {
							close(response)
							exit = true
							break
						}
						bytes, err := json.Marshal(&msg)
						if err != nil {
							log.Printf("error %v while marshalling batch of message\n", err)
							continue
						}
						wsData := WSData{
							Topic: "logs",
							Data:  string(bytes),
						}
						bytes, err = json.Marshal(wsData)
						if err != nil {
							log.Printf("error %v while marshalling batch of message\n", err)
							continue
						}
						send <- bytes
						break
					}
				}
			}(ctx, response, c.send)

			err := mainStorage.FetchingLog(ctx, core.QueryLogOption{
				Tag:       query.Tag,
				LogLevel:  query.LogLevel,
				StartTime: query.Start,
				EndTime:   query.End,
				BatchSize: math.MaxInt32,
				Response:  response,
			})
			if err != nil {
				log.Printf("Can not unmarshal query from client %s\n", c.ws.RemoteAddr())
				return
			}
			break
		default:
			break
		}
	}
}

func (c *WsConnection) write(ctx context.Context) {
	exit := false
	for !exit {
		select {
		case bytes := <-c.send:
			_ = c.ws.WriteMessage(websocket.TextMessage, bytes)
			break
		case <-ctx.Done():
			if ctx.Err() != nil {
				exit = true
			}
			break
		}
	}
	log.Println("close connection")
}

func (c *WsConnection) close() {
	_ = c.ws.Close()
	close(c.send)
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
	ctx, cancel := context.WithCancel(r.Context())
	defer func() {
		cancel()
		wsc.close()
	}()
	go wsc.write(ctx)
	wsc.read(ctx)
}
