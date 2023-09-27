package handlers

import (
	"github.com/zatrasz75/Service/pkg/storage"
	"net/http"
)

type Server struct {
	Server *http.Server
	Db     storage.Database
}
