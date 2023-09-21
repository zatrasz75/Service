package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/zatrasz75/Service/pkg/service"
	"log"
	"os"
)

type Database struct {
	ConnStr string //postgres://postgres:postgrespw@localhost:49153/Account

	Host     string // postgres
	User     string // postgres
	Password string // postgrespw
	Url      string // localhost
	Name     string // Account
	Port     string // 49153
}

// init вызывается перед main()
func init() {
	// загружает значения из файла .env в систему
	if err := godotenv.Load(); err != nil {
		log.Print("Файл .env не найден.")
	}
}

func main() {
	connstr := postgresDB()
	fmt.Println(connstr)

	//db, err := postgres.New(connstr)
	//if err != nil {
	//	log.Fatal("нет соединения с PostgresSQL", err)
	//}
	//err = db.CreateDataTable()
	//if err != nil {
	//	log.Fatal("не удалось создать таблицу session_token", err)
	//}

	brokers := []string{"localhost:9092"}
	topic := "my-topic"
	groupID := "my-group"

	service.Start(brokers, topic, groupID, connstr)

}

// Подключение к postgreSQL
func postgresDB() string {
	c := &Database{
		Host:     os.Getenv("HOST_DB"),
		User:     os.Getenv("USER_DB"),
		Password: os.Getenv("PASSWORD_DB"),
		Url:      os.Getenv("URL_DB"),
		Name:     os.Getenv("NAME_DB"),
		Port:     os.Getenv("PORT_DB"),
	}
	connstr := fmt.Sprintf(
		"%s://%s:%s@%s:%s/%s?sslmode=disable",
		c.Host, c.User, c.Password, c.Url, c.Port, c.Name)
	return connstr
}
