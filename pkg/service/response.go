package service

import (
	"encoding/json"
	"fmt"
	"github.com/zatrasz75/Service/pkg/logger"
	"github.com/zatrasz75/Service/pkg/storage"
	"net/http"
)

func getAge(name string) (int, error) {
	ageURL := fmt.Sprintf("https://api.agify.io/?name=%s", name)
	ageResponse, err := http.Get(ageURL)
	if err != nil {
		logger.Error("Ошибка при запросе возраста:", err)
		return 0, err
	}
	defer ageResponse.Body.Close()

	var ageData storage.Data
	err = json.NewDecoder(ageResponse.Body).Decode(&ageData)
	if err != nil {
		logger.Error("Ошибка при чтении ответа возраста:", err)
		return 0, err
	}

	return ageData.Age, err
}

func getGender(name string) (string, error) {
	genderURL := fmt.Sprintf("https://api.genderize.io/?name=%s", name)
	genderResponse, err := http.Get(genderURL)
	if err != nil {
		logger.Error("Ошибка при запросе пола:", err)
		return "", err
	}
	defer genderResponse.Body.Close()

	var genderData storage.Data
	err = json.NewDecoder(genderResponse.Body).Decode(&genderData)
	if err != nil {
		logger.Error("Ошибка при чтении ответа пола:", err)
		return "", err
	}

	return genderData.Gender, err
}

func getNationalities(name string) (string, error) {
	nationalityURL := fmt.Sprintf("https://api.nationalize.io/?name=%s", name)
	nationalityResponse, err := http.Get(nationalityURL)
	if err != nil {
		logger.Error("Ошибка при запросе национальности:", err)
		return "", err
	}
	defer nationalityResponse.Body.Close()

	var nationalityData storage.Data
	err = json.NewDecoder(nationalityResponse.Body).Decode(&nationalityData)
	if err != nil {
		logger.Error("Ошибка при чтении ответа национальности:", err)
		return "", err
	}

	return nationalityData.Nationality, err
}
