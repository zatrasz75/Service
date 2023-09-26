package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const maxLogFileSize = 100 * 1024 * 1024 // 100 MB

var logger *log.Logger
var logFile *os.File
var fileSizeMutex sync.Mutex

func init() {
	filePath := "app.log"
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Ошибка открытия файла журнала:", err)
	}
	logFile = file
	logger = log.New(logFile, "", log.Ldate|log.Ltime|log.Lmicroseconds)

	// Запускаем горутину для проверки размера файла и его обработки
	go checkAndRotateLogFile()
}

func logWithCallerInfo(file string, line int, level string, message string, args ...interface{}) {
	caller := fmt.Sprintf("%s:%d", filepath.Base(file), line)
	messageWithCaller := fmt.Sprintf("[%s] %s %s %s", level, getFormattedTime(), caller, fmt.Sprintf(message, args...))
	logger.Println(messageWithCaller)
	fmt.Println(messageWithCaller)
}

// Error записывает сообщение об ошибке в лог вместе с контекстом вызова функции.
// Параметр err содержит ошибку, связанную с данным сообщением.
func Error(message string, err error) {
	_, file, line, _ := runtime.Caller(1)
	logWithCallerInfo(file, line, "ERROR", "%s: %v", message, err)
}

// Info записывает информационное сообщение в лог вместе с контекстом вызова функции.
// Параметры args содержат дополнительные данные для сообщения.
func Info(message string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	logWithCallerInfo(file, line, "INFO", message, args...)
}

// Fatal записывает фатальное сообщение в лог вместе с контекстом вызова функции
// и завершает приложение с кодом ошибки 1.
// Параметр err содержит ошибку, связанную с данным сообщением.
func Fatal(message string, err error) {
	_, file, line, _ := runtime.Caller(1)
	logWithCallerInfo(file, line, "FATAL", "%s: %v", message, err)
	os.Exit(1) // Завершаем приложение с кодом ошибки
}

// Debug записывает информационное сообщение в лог вместе с контекстом вызова функции.
// Параметры args содержат дополнительные данные для сообщения.
func Debug(message string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	logWithCallerInfo(file, line, "DEBUG", message, args...)
}

// getFormattedTime возвращает текущее время в заданном формате.
func getFormattedTime() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

// checkAndRotateLogFile проверяет размер файла лога и при необходимости выполняет его ротацию.
func checkAndRotateLogFile() {
	for {
		time.Sleep(time.Minute) // Пауза для проверки раз в минуту (настраивайте по желанию)
		fileSizeMutex.Lock()
		stat, err := logFile.Stat()
		if err != nil {
			log.Println("Ошибка при получении информации о файле логов:", err)
			fileSizeMutex.Unlock()
			time.Sleep(time.Minute) // Делаем паузу если ошибка
			continue
		}

		if stat.Size() > maxLogFileSize {
			logFile.Close()
			err = os.Rename("app.log", "app.old.log")
			if err != nil {
				log.Println("Ошибка при переименовании файла логов:", err)
			}
			newFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				log.Println("Ошибка при открытии нового файла журнала:", err)
			} else {
				logFile = newFile
				logger.SetOutput(logFile)
			}
		}
		fileSizeMutex.Unlock()
	}
}
