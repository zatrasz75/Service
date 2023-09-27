package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/zatrasz75/Service/configs"
	"github.com/zatrasz75/Service/pkg/api"
	"github.com/zatrasz75/Service/pkg/logger"
)

//type Database struct {
//	ConnStr string //postgres://postgres:postgrespw@localhost:49153/Account
//
//	Host     string // postgres
//	User     string // postgres
//	Password string // postgrespw
//	Url      string // localhost
//	Name     string // Account
//	Port     string // 49153
//}

// init вызывается перед main()
func init() {
	// загружает значения из файла .env в систему
	if err := godotenv.Load(); err != nil {
		logger.Error("Файл .env не найден.", err)
	}
}

func main() {
	//connstr := postgresDB()
	//
	//brokers := []string{"localhost:9092"}
	//topic := "FIO"
	//topicErr := "FIO_FAILED"
	//groupID := "FIO"
	//
	//service.Start()

	//----------------------------------------------------------------------

	cfg := configs.New()
	fmt.Println(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.TopicErr, cfg.Kafka.GroupID)

	// экземпляр api
	httpServer := api.New()
	err := httpServer.Run()
	if err != nil {
		logger.Fatal("Ошибка при запуске сервера:", err)
	}

	api.GraceShutdown(httpServer)

}

//// Подключение к postgreSQL
//func postgresDB() string {
//	c := &Database{
//		Host:     os.Getenv("HOST_DB"),
//		User:     os.Getenv("USER_DB"),
//		Password: os.Getenv("PASSWORD_DB"),
//		Url:      os.Getenv("URL_DB"),
//		Name:     os.Getenv("NAME_DB"),
//		Port:     os.Getenv("PORT_DB"),
//	}
//	connstr := fmt.Sprintf(
//		"%s://%s:%s@%s:%s/%s?sslmode=disable",
//		c.Host, c.User, c.Password, c.Url, c.Port, c.Name)
//	return connstr
//}
