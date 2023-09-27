package api

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/zatrasz75/Service/configs"
	"github.com/zatrasz75/Service/pkg/handlers"
	"github.com/zatrasz75/Service/pkg/logger"
	"github.com/zatrasz75/Service/pkg/storage"
	"github.com/zatrasz75/Service/pkg/storage/postgres"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

// API представляет собой приложение с набором обработчиков.
type API struct {
	r      *mux.Router // Маршрутизатор запросов
	port   string      // Порт
	host   string      // Хост
	srv    *http.Server
	PG     storage.Database // база данных
	server *handlers.Server
}

// Router возвращает маршрутизатор запросов.
func (api *API) Router() *mux.Router {
	return api.r
}

func New() *API {
	// Конфигурация
	cfg := configs.New()
	PG, err := postgres.New(cfg.DataBase.ConnStr)
	if err != nil {
		logger.Fatal("нет соединения с PostgresSQL", err)
	}
	err = PG.CreateDataTable()
	if err != nil {
		logger.Fatal("не удалось создать таблицу", err)
	}

	api := &API{
		r:      mux.NewRouter(),
		host:   cfg.Server.AddrHost,
		port:   cfg.Server.AddrPort,
		PG:     PG,
		server: &handlers.Server{PG: PG},
	}
	// Регистрируем обработчики API.
	api.endpoints()

	return api
}

// Run Метод для запуска сервера
func (api *API) Run() error {
	// Конфигурация
	cfg := configs.New()

	api.srv = &http.Server{
		Addr:         api.host + ":" + api.port,
		Handler:      api.r,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}
	logger.Info("Запуск сервера на http://" + api.srv.Addr)

	go func() {
		err := api.srv.ListenAndServe()
		if err != nil {
			logger.Error("Остановка сервера", err)
			return
		}
	}()

	return nil
}

// Stop Метод для остановки сервера
func (api *API) Stop() error {
	// Конфигурация
	cfg := configs.New()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTime)
	defer cancel()
	err := api.srv.Shutdown(ctx)
	if err != nil {
		logger.Error("Shutdown ошибка при попытке остановить сервер", err)
		return err
	}

	return nil
}

// GraceShutdown Выключает сервер при получении сигнала об остановке
func GraceShutdown(httpServer *API) {
	quitCH := make(chan os.Signal, 1)
	signal.Notify(quitCH, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quitCH

	err := shutdownServer(httpServer)
	if err != nil {
		logger.Fatal("Ошибка при остановке сервера:", err)
	}
}

func shutdownServer(httpServer *API) error {
	err := httpServer.Stop()
	if err != nil {
		logger.Error("Ошибка при закрытии прослушивателей или тайм-аут контекста: %v", err)
	}

	logger.Info("Сервер успешно выключен")

	return nil
}

// Регистрация обработчиков API.
func (api *API) endpoints() {
	api.r.HandleFunc("/data", api.server.GetData).Methods(http.MethodGet)
	api.r.HandleFunc("/data", api.server.AddData).Methods(http.MethodPost)
	api.r.HandleFunc("/data/{id}", api.server.DeleteData).Methods(http.MethodDelete)
	api.r.HandleFunc("/data/{id}", api.server.UpdateData).Methods(http.MethodPut)
	api.r.HandleFunc("/data/{id}", api.server.PartialUpdateData).Methods(http.MethodPatch)
}
