package postgres

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/zatrasz75/Service/pkg/storage"
	"log"
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
		log.Printf("не удалось создать таблицу service_data %s", err)
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
