package main

import (
	"github.com/joho/godotenv"
	"github.com/zatrasz75/Service/configs"
	"github.com/zatrasz75/Service/pkg/api"
	"github.com/zatrasz75/Service/pkg/logger"
	"github.com/zatrasz75/Service/pkg/service"
)

// init вызывается перед main() и загружает значения из файла .env в систему
func init() {
	if err := godotenv.Load(); err != nil {
		logger.Error("Файл .env не найден.", err)
	}
}

func main() {
	cfg := configs.New()

	// Каналы для управления остановкой приложений
	kafkaDoneCh := make(chan struct{})
	serverDoneCh := make(chan struct{})

	// Экземпляр API
	httpServer := api.New()

	// Запуск сервера в горутине
	go func() {
		err := httpServer.Run()
		if err != nil {
			logger.Fatal("Ошибка при запуске сервера:", err)
		}
		close(serverDoneCh)
	}()

	// Запуск сервиса Kafka в горутине
	go func() {
		err := service.Start(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.TopicErr, cfg.Kafka.GroupID, cfg.DataBase.ConnStr)
		if err != nil {
			logger.Fatal("Не удалось запустить сервис Kafka", err)
		}
		close(kafkaDoneCh)
	}()

	// Ожидание сигнала завершения работы Kafka и сервера
	select {
	case <-kafkaDoneCh: // Kafka завершила работу
	case <-serverDoneCh: // Сервер завершил работу
	}

	api.GraceShutdown(httpServer)

}
