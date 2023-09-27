package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/zatrasz75/Service/pkg/logger"
	"github.com/zatrasz75/Service/pkg/storage"
	"github.com/zatrasz75/Service/pkg/storage/postgres"
	"regexp"
	"sync"
)

// Client — клиент Kafka.
type Client struct {
	// Reader осуществляет операции чтения топика.
	Reader *kafka.Reader

	// Writer осуществляет операции записи в топики.
	Writer      *kafka.Writer
	ErrorWriter *kafka.Writer

	// Broker адрес брокера Kafka.
	Broker string
}

// New создаёт и инициализирует клиента Kafka.
// Функция-конструктор.
func New(brokers []string, topic string, topicErr string, groupId string) (*Client, error) {
	if len(brokers) == 0 || topic == "" || groupId == "" || topicErr == "" {
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
	// Сохраняем адрес брокера.
	c.Broker = brokers[0]

	// Инициализация компонента отправки сообщений в топик FIO.
	c.Writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers[0]),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	// Инициализация компонента отправки сообщений в топик FIO_FAILED.
	c.ErrorWriter = &kafka.Writer{
		Addr:     kafka.TCP(brokers[0]),
		Topic:    topicErr,
		Balancer: &kafka.LeastBytes{},
	}

	return &c, nil
}

// sendErrorMessage отправляет сообщение с ошибкой в очередь FIO_FAILED.
func (c *Client) sendErrorMessage(msg kafka.Message, errorTopic string, fio storage.Data) error {
	var r storage.Data
	if err := json.Unmarshal(msg.Value, &r); err != nil {
		logger.Error("Ошибка разбора JSON: ", err)
	}
	r.Name = fio.Name
	r.Surname = fio.Surname
	r.Patronymic = fio.Patronymic
	r.Err = fio.Err

	errorMessageJSON, err := json.Marshal(r)
	if err != nil {
		logger.Error("Ошибка маршалирования JSON: %v\n", err)
		return err
	}

	// Создаем новый kafka.Writer с топиком FIO_FAILED.
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{c.Broker},
		Topic:   errorTopic,
	})

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: errorMessageJSON,
	})
	if err != nil {
		logger.Error("Ошибка отправки сообщения с ошибкой: %v\n", err)
	}
	// Закрываем writer.
	writer.Close()

	return err
}

// Проверяет, что каждое слово начинается с заглавной буквы и состоит из русских букв.
func isRussianCapital(name string) bool {
	regex := regexp.MustCompile(`^[А-Я][а-яА-Я]*$`)
	return regex.MatchString(name)
}

// validateAndEnrichMessage выполняет проверку сообщения с ФИО.
func validateAndEnrichMessage(input storage.Data) (storage.Data, error) {
	if input.Name == "" || input.Surname == "" {
		return storage.Data{}, errors.New("некорректное сообщение: отсутствуют обязательные поля")
	}

	if !isRussianCapital(input.Name) || !isRussianCapital(input.Surname) {
		return storage.Data{}, errors.New("некорректное сообщение: ФИО должно содержать только русские буквы и начинаться с заглавной буквы")
	}

	return input, nil
}

// fetchProcessCommit сначала выбирает сообщение из очереди,
// потом обрабатывает, после чего подтверждает.
func (c *Client) fetchProcessCommit(db storage.Database) error {
	for {
		msg, err := c.Reader.FetchMessage(context.Background())
		if err != nil {
			logger.Error("Ошибка получения сообщения из Kafka: ", err)
			return err
		}

		var r storage.Data
		err = json.Unmarshal(msg.Value, &r)
		if err != nil {
			logger.Error("Ошибка разбора JSON: %v\n", err)
		}
		_, err = validateAndEnrichMessage(r)
		if err != nil {
			// отправляем сообщение с ошибкой в FIO_FAILED
			r.Err = err.Error()
			fmt.Println(r.Err)
			err = c.sendErrorMessage(msg, "FIO_FAILED", r)
		} else {
			var wg sync.WaitGroup
			wg.Add(3)

			go func() {
				defer wg.Done()
				age, err := getAge(r.Name)
				if err != nil {
					logger.Error("не удалось выполнить запрос ", err)
				}
				r.Age = age
				logger.Info("Возраст: %d", age)
			}()
			go func() {
				defer wg.Done()
				gender, err := getGender(r.Name)
				if err != nil {
					logger.Error("не удалось выполнить запрос ", err)
				}
				r.Gender = gender
				logger.Info("Пол: %s", gender)
			}()
			go func() {
				defer wg.Done()
				nationalities, err := getNationalities(r.Name)
				if err != nil {
					logger.Error("не удалось выполнить запрос ", err)
				}
				r.Nationality = nationalities
				logger.Info("Национальность: %s", nationalities)
			}()

			wg.Wait()

			// сохраняем обогащенные данные в базу
			_, err = db.SaveDataToDatabase(r)
			if err != nil {
				logger.Error("не получилось сохранить данные в базу данных", err)
				return err
			}
		}

		// Подтверждение сообщения как обработанного.
		err = c.Reader.CommitMessages(context.Background(), msg)
		return err
	}
}

func Start(brokers []string, topic, topicErr, groupID, connstr string) error {
	// Инициализация клиента Kafka.
	kfk, err := New(brokers, topic, topicErr, groupID)
	if err != nil {
		logger.Error("не удалось запустить сервис", err)
		return err
	}

	// Инициализация базы данных.
	db, err := postgres.New(connstr)
	if err != nil {
		logger.Error("нет соединения с PostgresSQL", err)
		return err
	}

	// чтение следующего сообщения.
	go func() {
		for {
			err = kfk.fetchProcessCommit(db)
			if err != nil {
				logger.Error("не удалось прочитать сообщение", err)
			}
		}
	}()

	// Ожидаем завершения работы программы.
	select {}

}
