package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	redisClient "github.com/shreyas/dronc/lib/redis"
)

// Setup registers all HTTP routes
func Setup() *gin.Engine {
	router := gin.Default()

	router.GET("/", rootHandler)
	router.GET("/health", healthHandler)
	router.GET("/health/deep", deepHealthHandler)

	return router
}

func rootHandler(c *gin.Context) {
	c.String(http.StatusOK, "Dronc - Redis-backed Job Scheduler")
}

func healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

func deepHealthHandler(c *gin.Context) {
	// Check Redis connection
	if err := redisClient.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"checks": gin.H{
				"redis": "unreachable",
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
		"checks": gin.H{
			"redis": "healthy",
		},
	})
}
