package storage

type Data struct {
	Name        string `json:"name"`
	Surname     string `json:"surname"`
	Patronymic  string `json:"patronymic"`
	Age         string `json:"age"`
	Gender      string `json:"gender"`
	Nationality string `json:"nationality"`
}

type Database interface {
	CreateDataTable() error
	SaveDataToDatabase(d Data) (int, error)
}
