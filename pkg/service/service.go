package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"github.com/zatrasz75/Service/configs"
	"github.com/zatrasz75/Service/pkg/logger"
	"github.com/zatrasz75/Service/pkg/storage"
	"github.com/zatrasz75/Service/pkg/storage/postgres"
	"regexp"
	"sync"
	"time"
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
func New() (*Client, error) {
	cfg := configs.New()
	if len(cfg.Kafka.Brokers) == 0 || cfg.Kafka.Topic == "" || cfg.Kafka.GroupID == "" || cfg.Kafka.TopicErr == "" {
		return nil, errors.New("не указаны параметры подключения к Kafka")
	}

	c := Client{}

	// Инициализация компонента получения сообщений.
	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.GroupID,
		MinBytes: 10e1,
		MaxBytes: 10e6,
	})
	// Сохраняем адрес брокера.
	c.Broker = cfg.Kafka.Brokers[0]

	// Инициализация компонента отправки сообщений в топик FIO.
	c.Writer = &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers[0]),
		Topic:    cfg.Kafka.Topic,
		Balancer: &kafka.LeastBytes{},
	}
	// Инициализация компонента отправки сообщений в топик FIO_FAILED.
	c.ErrorWriter = &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers[0]),
		Topic:    cfg.Kafka.TopicErr,
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
		logger.Error("Ошибка отправки сообщения: %v\n", err)
	}

	return err
}

// sendErrorMessage отправляет сообщение с ошибкой в очередь FIO_FAILED.
func (c *Client) sendErrorMessage(msg kafka.Message, errorTopic string, fio storage.Data) error {
	var r storage.Data
	if err := json.Unmarshal(msg.Value, &r); err != nil {
		logger.Error("Ошибка разбора JSON: %v\n", err)
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
			logger.Error("Ошибка получения сообщения из Kafka: %v\n", err)
			return err
		}

		var r storage.Data
		err = json.Unmarshal(msg.Value, &r)
		if err != nil {
			logger.Error("Ошибка разбора JSON: %v\n", err)
		}
		_, err = validateAndEnrichMessage(r)
		if err != nil {
			//fmt.Printf("Имя: %s\n", r.Name)
			//fmt.Printf("Фамилия: %s\n", r.Surname)
			//fmt.Printf("Отчество: %s\n", r.Patronymic)
			//fmt.Printf("Лет: %s\n", r.Age)
			//fmt.Printf("Пол: %s\n", r.Gender)
			//fmt.Printf("Национальность: %s\n", r.Nationality)
			//fmt.Printf("UpsErr: %s\n", r.Err)
			r.Err = err.Error()
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

			//fmt.Printf("Имя: %s\n", r.Name)
			//fmt.Printf("Фамилия: %s\n", r.Surname)
			//fmt.Printf("Отчество: %s\n", r.Patronymic)
			//fmt.Printf("Лет: %d\n", r.Age)
			//fmt.Printf("Пол: %s\n", r.Gender)
			//fmt.Printf("Национальность: %s\n", r.Nationality)
			//
			//fmt.Printf("Err: %s\n", r.Err)

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

func Start() {
	cfg := configs.New()
	// Инициализация клиента Kafka.
	kfk, err := New()
	if err != nil {
		logger.Fatal("не удалось запустить сервис", err)
	}

	// Инициализация базы данных.
	db, err := postgres.New(cfg.DataBase.ConnStr)
	if err != nil {
		logger.Fatal("нет соединения с PostgresSQL", err)
	}
	err = db.CreateDataTable()
	if err != nil {
		logger.Fatal("не удалось создать таблицу session_token", err)
	}

	data := storage.Data{
		Name:       "Михаил",
		Surname:    "Токмачев",
		Patronymic: "Владимирович",
		//Age:         "48",
		//Gender:      "Мужской",
		//Nationality: "Русский",
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Error("Ошибка маршалирования JSON: %v\n", err)
		return
	}

	// Отправка сообщения.
	go func() {
		for range time.Tick(time.Second * 5) {
			err = kfk.sendMessages(jsonData)
			if err != nil {
				logger.Error("не удалось отправить сообщение", err)
			}
		}
	}()

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
