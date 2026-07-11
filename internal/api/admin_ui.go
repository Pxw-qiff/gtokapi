package api

import (
	_ "embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed admin_panel.html
var adminPanelHTML []byte

// handleAdminPanel 返回管理面板 HTML 页面（无需认证，页面内通过 admin key 认证 API）。
func (s *Server) handleAdminPanel(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", adminPanelHTML)
}