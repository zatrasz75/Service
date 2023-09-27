package storage

type Data struct {
	Name       string `json:"name"`
	Surname    string `json:"surname"`
	Patronymic string `json:"patronymic"`

	Age         int    `json:"age"`
	Gender      string `json:"gender"`
	Nationality string `json:"nationality"`

	Err string `json:"err"`
}

type UsersData struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Surname     string `json:"surname"`
	Patronymic  string `json:"patronymic"`
	Age         int    `json:"age"`
	Gender      string `json:"gender"`
	Nationality string `json:"nationality"`
}

type Database interface {
	CreateDataTable() error
	SaveDataToDatabase(d Data) (int, error)
	Select(genderFilter string, page, pageSize int) ([]UsersData, error)
	DeleteDataByID(id int) error
	UpdateDataByID(id int, newData UsersData) error
	PartialUpdateDataByID(id int, partialData map[string]interface{}) error
}
