package main
import "github.com/redis/go-redis/v9"
import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
	"github.com/joho/godotenv"
	"golang.org/x/time/rate"
	"os"
	"github.com/coder/websocket"
	"server/ent"
	_ "github.com/lib/pq"


	
)

// chatServer enables broadcasting to a set of subscribers.
type chatServer struct {
	// subscriberMessageBuffer controls the max number
	// of messages that can be queued for a subscriber
	// before it is kicked.
	//
	// Defaults to 16.
	subscriberMessageBuffer int

	// publishLimiter controls the rate limit applied to the publish endpoint.
	//
	// Defaults to one publish every 100ms with a burst of 8.
	publishLimiter *rate.Limiter

	// logf controls where logs are sent.
	// Defaults to log.Printf.
	logf func(f string, v ...any)

	// serveMux routes the various endpoints to the appropriate handler.
	serveMux http.ServeMux

	subscribersMu sync.Mutex
	subscribers   map[*subscriber]struct{}
    redisClient  *redis.Client
    redisSub     *redis.PubSub
	db 		 *ent.Client
	producer *KafkaProducer
}
func (cs *chatServer) listenRedis() {
    ch := cs.redisSub.Channel()
    for msg := range ch {
		cs.logf("received message from Redis: %s", msg.Payload)
        cs.publish([]byte(msg.Payload)) // Reuse existing in-memory broadcast
    }
}

// newChatServer constructs a chatServer with the defaults.
func newChatServer() *chatServer {
    client, sub := SetupRedisClient()
	 // Load DSN
    _ = godotenv.Load()
    dsn := os.Getenv("dsn")
    db, err := ent.Open("postgres", dsn)
    if err != nil {
        log.Fatalf("failed to connect to postgres: %v", err)
    }

    // Run migration
    if err := db.Schema.Create(context.Background()); err != nil {
        log.Fatalf("failed creating schema: %v", err)
    }
	prod, err := NewKafkaProducer()
    if err != nil {
        log.Fatalf("failed to create Kafka producer: %v", err)
    }
	cs := &chatServer{
		subscriberMessageBuffer: 16,
		logf:                    log.Printf,
		subscribers:             make(map[*subscriber]struct{}),
		publishLimiter:          rate.NewLimiter(rate.Every(time.Millisecond*100), 8),
        redisClient:             client,
        redisSub:                sub,
		db:                      db,
		producer: 			  prod,


	}       
	cs.serveMux.Handle("/", http.FileServer(http.Dir(".")))
	cs.serveMux.HandleFunc("/subscribe", cs.subscribeHandler)
	cs.serveMux.HandleFunc("/publish", cs.publishHandler)
    go cs.listenRedis()
	return cs
}

// subscriber represents a subscriber.
// Messages are sent on the msgs channel and if the client
// cannot keep up with the messages, closeSlow is called.
type subscriber struct {
	msgs      chan []byte
	closeSlow func()
}

func (cs *chatServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Basic CORS for development
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	cs.serveMux.ServeHTTP(w, r)
}

// subscribeHandler accepts the WebSocket connection and then subscribes
// it to all future messages.
func (cs *chatServer) subscribeHandler(w http.ResponseWriter, r *http.Request) {
	err := cs.subscribe(w, r)
	if errors.Is(err, context.Canceled) {
		return
	}
	if websocket.CloseStatus(err) == websocket.StatusNormalClosure ||
		websocket.CloseStatus(err) == websocket.StatusGoingAway {
		return
	}
	if err != nil {
		cs.logf("%v", err)
		return
	}
}
// consumeToRedis reads from Kafka and forwards to Redis
func (cs *chatServer) consumeToRedis() {
    consumer, err := NewKafkaConsumer("redis-consumer")
    if err != nil {
        log.Fatalf("failed to create Kafka consumer: %v", err)
    }
    defer consumer.reader.Close()

    ctx := context.Background()
    err = consumer.Consume(ctx, func(msg string) error {
        log.Printf("Kafka → Redis: %s", msg)
        return cs.redisClient.Publish(ctx, "my-channel", msg).Err()
    })
    if err != nil {
        log.Printf("consumer error: %v", err)
    }
}

// consumeToDB reads from Kafka and saves messages into Postgres
func (cs *chatServer) consumeToDB() {
    consumer, err := NewKafkaConsumer("db-consumer")
    if err != nil {
        log.Fatalf("failed to create Kafka consumer: %v", err)
    }
    defer consumer.reader.Close()

    ctx := context.Background()
    err = consumer.Consume(ctx, func(msg string) error {
        log.Printf("Kafka → DB: %s", msg)
        _, dbErr := cs.db.Message.
            Create().
            SetText(msg).
            Save(ctx)
        if dbErr != nil {
            log.Printf("failed saving message: %v", dbErr)
        }
        return nil
    })
    if err != nil {
        log.Printf("consumer error: %v", err)
    }
}

// publishHandler reads the request body with a limit of 8192 bytes and then publishes
// the received message.
func (cs *chatServer) publishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	body := http.MaxBytesReader(w, r.Body, 8192)
	msg, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
		return
	}

	// Log the received message
	cs.logf("received messages: %s", string(msg))


	// Save to DB
	// _, err = cs.db.Message.
	// 	Create().
	// 	SetText(string(msg)).
	// 	Save(r.Context())
	// if err != nil {
	// 	http.Error(w, "Failed to save message", http.StatusInternalServerError)
	// 	return
	// }
	err = cs.producer.ProduceMessage(string(msg))
	if err != nil {
		http.Error(w, "Failed to produce message to Kafka", http.StatusInternalServerError)
		return
	}

	// Still publish to Redis
	// err = cs.redisClient.Publish(r.Context(), "my-channel", msg).Err()
	// if err != nil {
	// 	http.Error(w, "Failed to publish message", http.StatusInternalServerError)
	// 	return
	// }

		
		w.WriteHeader(http.StatusAccepted)
	}

// subscribe subscribes the given WebSocket to all broadcast messages.
// It creates a subscriber with a buffered msgs chan to give some room to slower
// connections and then registers the subscriber. It then listens for all messages
// and writes them to the WebSocket. If the context is cancelled or
// an error occurs, it returns and deletes the subscription.
//
// It uses CloseRead to keep reading from the connection to process control
// messages and cancel the context if the connection drops.
func (cs *chatServer) subscribe(w http.ResponseWriter, r *http.Request) error {
	var mu sync.Mutex
	var c *websocket.Conn
	var closed bool
	s := &subscriber{
		msgs: make(chan []byte, cs.subscriberMessageBuffer),
		closeSlow: func() {
			mu.Lock()
			defer mu.Unlock()
			closed = true
			if c != nil {
				c.Close(websocket.StatusPolicyViolation, "connection too slow to keep up with messages")
			}
		},
	}
	cs.addSubscriber(s)
	defer cs.deleteSubscriber(s)

	c2, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Allow all origins for development; tighten in production
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return err
	}
	mu.Lock()
	if closed {
		mu.Unlock()
		return net.ErrClosed
	}
	c = c2
	mu.Unlock()
	defer c.CloseNow()

	ctx := c.CloseRead(context.Background())
	  // **NEW: Send historical messages from DB**
    messages, err := cs.db.Message.
        Query().
        Order(ent.Asc("id")).  // or use a created_at field
        Limit(100).            // last 100 messages
        All(ctx)
    if err != nil {
        cs.logf("failed to fetch messages: %v", err)
    } else {
        for _, msg := range messages {
            err := writeTimeout(ctx, time.Second*5, c, []byte(msg.Text))
            if err != nil {
                return err
            }
        }
    }

	for {
		select {
		case msg := <-s.msgs:
			err := writeTimeout(ctx, time.Second*5, c, msg)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// publish publishes the msg to all subscribers.
// It never blocks and so messages to slow subscribers
// are dropped.
func (cs *chatServer) publish(msg []byte) {
	cs.subscribersMu.Lock()
	defer cs.subscribersMu.Unlock()

	cs.publishLimiter.Wait(context.Background())

	for s := range cs.subscribers {
		select {
		case s.msgs <- msg:
		default:
			go s.closeSlow()
		}
	}
}

// addSubscriber registers a subscriber.
func (cs *chatServer) addSubscriber(s *subscriber) {
	cs.subscribersMu.Lock()
	cs.subscribers[s] = struct{}{}
	cs.subscribersMu.Unlock()
}

// deleteSubscriber deletes the given subscriber.
func (cs *chatServer) deleteSubscriber(s *subscriber) {
	cs.subscribersMu.Lock()
	delete(cs.subscribers, s)
	cs.subscribersMu.Unlock()
}

func writeTimeout(ctx context.Context, timeout time.Duration, c *websocket.Conn, msg []byte) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return c.Write(ctx, websocket.MessageText, msg)
}

func main() {
	cs := newChatServer()
	    // Consumer A: Kafka → Redis
    go cs.consumeToRedis()

    // Consumer B: Kafka → Postgres
    go cs.consumeToDB()
	log.Printf("starting server on :8080")
	if err := http.ListenAndServe(":8080", cs); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
