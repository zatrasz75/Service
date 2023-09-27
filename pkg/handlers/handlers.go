package handlers

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/zatrasz75/Service/pkg/logger"
	"github.com/zatrasz75/Service/pkg/storage"
	"net/http"
	"strconv"
)

type Server struct {
	Server *http.Server
	PG     storage.Database
}

// Вспомогательная функция для преобразования строки в число с проверкой ошибок.
func parseQueryParam(param string) int {
	value, err := strconv.Atoi(param)
	if err != nil || value <= 0 {
		// Если произошла ошибка или значение некорректное, используем значение по умолчанию.
		return 1
	}
	return value
}

// GetData Метод для обработки GET-запроса на эндпоинт /data.
func (s *Server) GetData(w http.ResponseWriter, r *http.Request) {
	// Получаем параметры запроса (фильтры и пагинация).
	genderFilter := r.URL.Query().Get("gender")
	pageStr := r.URL.Query().Get("page")
	pageSizeStr := r.URL.Query().Get("pageSize")

	page := parseQueryParam(pageStr)
	pageSize := parseQueryParam(pageSizeStr)

	// Вызываем функцию запроса в базу данных с фильтрами и пагинацией.
	data, err := s.PG.Select(genderFilter, page, pageSize)
	if err != nil {
		logger.Error("Ошибка при выполнении запроса к базе данных", err)
		http.Error(w, "Ошибка при выполнении запроса к базе данных", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// AddData Метод для обработки POST-запроса на эндпоинт /data.
func (s *Server) AddData(w http.ResponseWriter, r *http.Request) {
	var newData storage.Data
	err := json.NewDecoder(r.Body).Decode(&newData)
	if err != nil {
		logger.Error("Ошибка при чтении JSON", err)
		http.Error(w, "Ошибка при чтении JSON", http.StatusBadRequest)
		return
	}

	// Сохраняем новые данные в базу данных.
	id, err := s.PG.SaveDataToDatabase(newData)
	if err != nil {
		logger.Error("Ошибка при сохранении данных в базу данных", err)
		http.Error(w, "Ошибка при сохранении данных в базу данных", http.StatusInternalServerError)
		return
	}

	// Отправляем ответ с ID новой записи.
	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]int{"id": id}
	json.NewEncoder(w).Encode(response)
}

// DeleteData Метод для обработки DELETE-запроса на эндпоинт /data/{id}.
func (s *Server) DeleteData(w http.ResponseWriter, r *http.Request) {
	// Проверяем, что URL-путь содержит параметр {id}.
	idParam := mux.Vars(r)["id"]
	if idParam == "" {
		logger.Info("Отсутствует идентификатор")
		http.Error(w, "Отсутствует идентификатор", http.StatusBadRequest)
		return
	}

	id := parseQueryParam(idParam)

	// Вызываем функцию удаления данных из базы данных по идентификатору.
	err := s.PG.DeleteDataByID(id)
	if err != nil {
		logger.Error("Ошибка при удалении данных", err)
		http.Error(w, "Ошибка при удалении данных", http.StatusInternalServerError)
		return
	}

	// Отправляем успешный ответ.
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{"message": "Данные успешно удалены"}
	json.NewEncoder(w).Encode(response)
}

// UpdateData Обработчик для HTTP метода PUT для полного обновления сущности.
func (s *Server) UpdateData(w http.ResponseWriter, r *http.Request) {
	idParam := mux.Vars(r)["id"]
	id := parseQueryParam(idParam)

	// Чтение новых данных из тела запроса.
	var updatedData storage.UsersData
	err := json.NewDecoder(r.Body).Decode(&updatedData)
	if err != nil {
		logger.Error("Ошибка при чтении JSON", err)
		http.Error(w, "Ошибка при чтении JSON", http.StatusBadRequest)
		return
	}

	err = s.PG.UpdateDataByID(id, updatedData)
	if err != nil {
		logger.Error("Ошибка при обновлении данных", err)
		http.Error(w, "Ошибка при обновлении данных", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{"message": "Данные успешно обновлены"}
	json.NewEncoder(w).Encode(response)
}

// PartialUpdateData Обработчик для HTTP метода PATCH для частичного обновления сущности.
func (s *Server) PartialUpdateData(w http.ResponseWriter, r *http.Request) {
	idParam := mux.Vars(r)["id"]
	id := parseQueryParam(idParam)

	// Чтение частичных данных из тела запроса.
	var partialData map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&partialData)
	if err != nil {
		logger.Error("Ошибка при чтении JSON", err)
		http.Error(w, "Ошибка при чтении JSON", http.StatusBadRequest)
		return
	}

	err = s.PG.PartialUpdateDataByID(id, partialData)
	if err != nil {
		logger.Error("Ошибка при частичном обновлении данных", err)
		http.Error(w, "Ошибка при частичном обновлении данных", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{"message": "Данные успешно обновлены"}
	json.NewEncoder(w).Encode(response)
}
