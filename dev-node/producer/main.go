package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type OrderEvent struct {
	OrderID   string  `json:"order_id"`
	UserID    int64   `json:"user_id"`
	SellerID  int64   `json:"seller_id"`
	Amount    float64 `json:"amount"`
	EventTime int64   `json:"event_time"` // 毫秒时间戳（含乱序）
	ClientIP  string  `json:"client_ip"`
}

func main() {
	targetBroker := flag.String("broker", "datamac.local:9092", "数据机 Kafka 地址")
	qps := flag.Int("qps", 50000, "总吞吐目标 QPS")
	skew := flag.Bool("skew", true, "是否注入 85% 热点倾斜 Key")
	workers := flag.Int("workers", 8, "并发 Worker 协程数")
	flag.Parse()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(*targetBroker),
		Topic:        "user_orders",
		Balancer:     &kafka.Hash{}, // 基于 Key 进行 Hash 分区
		BatchSize:    2000,          // 批量大小
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	var sentCount uint64
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\n[Info] 收到停止信号，正在停止发压...")
		cancel()
	}()

	// 每秒吞吐监控
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var lastCount uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current := atomic.LoadUint64(&sentCount)
				delta := current - lastCount
				lastCount = current
				log.Printf("📊 [实时吞吐] %d records/sec | 累计发送: %d", delta, current)
			}
		}
	}()

	log.Printf("🚀 发压引擎启动 | Broker: %s | 目标 QPS: %d | 倾斜注入: %v | Workers: %d", *targetBroker, *qps, *skew, *workers)

	var wg sync.WaitGroup
	qpsPerWorker := *qps / *workers

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, writer, qpsPerWorker, *skew, &sentCount)
		}()
	}

	wg.Wait()
	log.Println("[Done] 造数引擎已完全关闭。")
}

func runWorker(ctx context.Context, writer *kafka.Writer, targetQps int, skew bool, counter *uint64) {
	interval := time.Second / time.Duration(targetQps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 1. 模拟 Key 倾斜：85% 集中在 3 个超级用户
			var userID int64
			if skew && r.Float64() < 0.85 {
				hotKeys := []int64{88888888, 99999999, 66666666}
				userID = hotKeys[r.Intn(len(hotKeys))]
			} else {
				userID = int64(r.Intn(10000000) + 100000)
			}

			// 2. 模拟事件乱序：10% 概率滞后 1~10 秒
			eventTime := time.Now().UnixMilli()
			if r.Float64() < 0.10 {
				eventTime -= int64(r.Intn(10000) + 1000)
			}

			event := OrderEvent{
				OrderID:   uuid.New().String(),
				UserID:    userID,
				SellerID:  userID % 1000,
				Amount:    float64(r.Intn(50000)) / 100.0,
				EventTime: eventTime,
				ClientIP:  fmt.Sprintf("192.168.%d.%d", r.Intn(255), r.Intn(255)),
			}

			payload, _ := json.Marshal(event)
			keyStr := fmt.Sprintf("%d", userID)

			err := writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(keyStr),
				Value: payload,
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("写入失败: %v", err)
			} else {
				atomic.AddUint64(counter, 1)
			}
		}
	}
}
