package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/zatrasz75/Service/pkg/storage"
	"github.com/zatrasz75/Service/pkg/storage/postgres"
	"log"
	"time"
)

// Client — клиент Kafka.
type Client struct {
	// Reader осуществляет операции чтения топика.
	Reader *kafka.Reader

	// Writer осуществляет операции записи в топики.
	Writer *kafka.Writer
}

// New создаёт и инициализирует клиента Kafka.
// Функция-конструктор.
func New(brokers []string, topic string, groupId string) (*Client, error) {
	if len(brokers) == 0 || topic == "" || groupId == "" {
		return nil, errors.New("не указаны параметры подключения к Kafka")
	}

	c := Client{}

	// Инициализация компонента получения сообщений.
	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 10e1,
		MaxBytes: 10e6,
	})

	// Инициализация компонента отправки сообщений.
	c.Writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers[0]),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	return &c, nil
}

// sendMessages отправляет сообщения в Kafka.
func (c *Client) sendMessages(messages []byte) error {
	kafkaMessage := kafka.Message{
		Value: messages,
	}

	err := c.Writer.WriteMessages(context.Background(), kafkaMessage)
	if err != nil {
		fmt.Printf("Ошибка отправки сообщения: %v\n", err)
	}

	return err
}

// fetchProcessCommit сначала выбирает сообщение из очереди,
// потом обрабатывает, после чего подтверждает.
func (c *Client) fetchProcessCommit(db storage.Database) error {
	// Выборка очередного сообщения из Kafka.
	for {
		msg, err := c.Reader.FetchMessage(context.Background())
		if err != nil {
			return err
		}

		// Обработка сообщения
		//fmt.Printf("%s\n", msg.Value)
		var r storage.Data
		err = json.Unmarshal(msg.Value, &r)
		if err != nil {
			fmt.Printf("Ошибка разбора JSON: %v\n", err)
		}
		fmt.Printf("Имя: %s\n", r.Name)
		fmt.Printf("Фамилия: %s\n", r.Surname)
		fmt.Printf("Отчество: %s\n", r.Patronymic)
		fmt.Printf("Лет: %s\n", r.Age)
		fmt.Printf("Пол: %s\n", r.Gender)
		fmt.Printf("Национальность: %s\n", r.Nationality)

		_, err = db.SaveDataToDatabase(r)
		if err != nil {
			return err
		}

		// Подтверждение сообщения как обработанного.
		err = c.Reader.CommitMessages(context.Background(), msg)
		return err
	}
}

func Start(brokers []string, topic, groupID, connstr string) {
	// Инициализация клиента Kafka.
	kfk, err := New(brokers, topic, groupID)
	if err != nil {
		log.Fatal(err)
	}

	// Инициализация базы данных.
	db, err := postgres.New(connstr)
	if err != nil {
		log.Fatal("нет соединения с PostgresSQL", err)
	}
	err = db.CreateDataTable()
	if err != nil {
		log.Fatal("не удалось создать таблицу session_token", err)
	}

	data := storage.Data{
		Name:        "Михаил",
		Surname:     "Токмачев",
		Patronymic:  "Владимирович",
		Age:         "48",
		Gender:      "Мужской",
		Nationality: "Русский",
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Ошибка маршалирования JSON: %v\n", err)
		return
	}

	// Отправка сообщения.
	go func() {
		for range time.Tick(time.Second * 5) {
			err = kfk.sendMessages(jsonData)
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	// чтение следующего сообщения.
	go func() {
		for {
			err = kfk.fetchProcessCommit(db)
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	// Ожидаем завершения работы программы.
	select {}

}
