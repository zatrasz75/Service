package postgres

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/zatrasz75/Service/pkg/logger"
	"github.com/zatrasz75/Service/pkg/storage"
	"strconv"
	"time"
)

// Store Хранилище данных
type Store struct {
	db *pgxpool.Pool
}

// New Конструктор
func New(constr string) (*Store, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var db, err = pgxpool.Connect(ctx, constr)
	if err != nil {
		return nil, err
	}
	s := Store{
		db: db,
	}
	return &s, nil
}

func (s *Store) CreateDataTable() error {
	qwery := `CREATE TABLE IF NOT EXISTS "service_data" (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    surname VARCHAR(255),
    patronymic VARCHAR(255),
    age INT,
    gender VARCHAR(255),
    nationality VARCHAR(255)
);`

	_, err := s.db.Exec(context.Background(), qwery)
	if err != nil {
		logger.Error("не удалось создать таблицу service_data %s", err)
		return err
	}

	return nil
}

// SaveDataToDatabase сохраняет данные в базу данных и возвращает ее id.
func (s *Store) SaveDataToDatabase(d storage.Data) (int, error) {
	var id int
	err := s.db.QueryRow(context.Background(), `
		INSERT INTO service_data (name, surname, patronymic, age, gender, nationality)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id;
		`,
		d.Name,
		d.Surname,
		d.Patronymic,
		d.Age,
		d.Gender,
		d.Nationality,
	).Scan(&id)

	return id, err
}

// Select выполняет SQL-запрос для выборки данных из таблицы service_data с фильтрами и пагинацией.
func (s *Store) Select(genderFilter string, page, pageSize int) ([]storage.UsersData, error) {
	// Создаем SQL-запрос с учетом фильтра и пагинации.
	query := "SELECT * FROM service_data WHERE true"
	if genderFilter != "" {
		query += fmt.Sprintf(" AND gender = '%s'", genderFilter)
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", pageSize, (page-1)*pageSize)

	// Выполняем запрос к базе данных.
	rows, err := s.db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []storage.UsersData
	for rows.Next() {
		var data storage.UsersData
		if err = rows.Scan(&data.ID, &data.Name, &data.Surname, &data.Patronymic, &data.Age, &data.Gender, &data.Nationality); err != nil {
			return nil, err
		}
		result = append(result, data)
	}

	return result, nil
}

// DeleteDataByID Создаем SQL-запрос для удаления данных по идентификатору.
func (s *Store) DeleteDataByID(id int) error {
	delet := "DELETE FROM service_data WHERE id = $1"

	_, err := s.db.Exec(context.Background(), delet, id)
	if err != nil {
		return err
	}

	return nil
}

// UpdateDataByID обновляет данные сущности по ее идентификатору.
func (s *Store) UpdateDataByID(id int, newData storage.UsersData) error {
	_, err := s.db.Exec(context.Background(), `
        UPDATE service_data 
        SET name = $2, surname = $3, patronymic = $4, age = $5, gender = $6, nationality = $7
        WHERE id = $1;
    `,
		id,
		newData.Name,
		newData.Surname,
		newData.Patronymic,
		newData.Age,
		newData.Gender,
		newData.Nationality,
	)
	return err
}

// PartialUpdateDataByID частично обновляет данные сущности по ее идентификатору.
func (s *Store) PartialUpdateDataByID(id int, partialData map[string]interface{}) error {
	// Динамический SQL-запрос на основе частичных данных.
	query := "UPDATE service_data SET"
	args := []interface{}{id}
	argIndex := 2 // Индекс первого аргумента после id.

	for key, value := range partialData {
		query += " " + key + " = $" + strconv.Itoa(argIndex) + ","
		args = append(args, value)
		argIndex++
	}

	// Удаление последней запятой из запроса.
	query = query[:len(query)-1]

	query += " WHERE id = $1;"
	_, err := s.db.Exec(context.Background(), query, args...)
	return err
}
