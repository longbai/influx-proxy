package main

import (
	"compress/gzip"
	"github.com/gin-gonic/gin"
	"net/http"
)

func (hs *HttpService) FalconRegister(mux *gin.Engine) {
	mux.POST("/api/push", hs.push)

	mux.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	mux.GET("/version", func(c *gin.Context) {
		c.String(http.StatusOK, "5.1.1")
	})

	mux.GET("/workdir", func(c *gin.Context) {
		c.String(http.StatusOK, "/home/")
	})

	mux.GET("/config", func(c *gin.Context) {
		c.JSON(http.StatusOK, "")
	})

	mux.GET("/config/reload", func(c *gin.Context) {
		c.String(http.StatusOK, "ok\n")
	})

	//mux.HandleFunc("/debug/connpool/", func(w http.ResponseWriter, r *http.Request) {
	//	_, _ = w.Write([]byte(""))
	//})

	// counter
	mux.GET("/counter/all", func(c *gin.Context) {
		c.JSON(http.StatusOK, "")
	})

	// TO BE DISCARDed
	//mux.HandleFunc("/statistics/all", func(w http.ResponseWriter, r *http.Request) {
	//	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	//	_, _ = w.Write([]byte(""))
	//})

	// step
	mux.GET("/proc/step", func(c *gin.Context) {
		c.JSON(http.StatusOK, "")
	})

	// trace
	mux.GET("/trace/", func(c *gin.Context) {
		c.JSON(http.StatusOK, "")
	})

	// filter
	mux.GET("/filter/", func(c *gin.Context) {
		c.JSON(http.StatusOK, "")
	})
}

type Dto struct {
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

type TransferResp struct {
	Msg        string
	Total      int
	ErrInvalid int
	Latency    int64
}

func (hs *HttpService) push(c *gin.Context) {
	req := c.Request
	defer req.Body.Close()
	if req.ContentLength == 0 {
		c.String(http.StatusBadRequest, "blank body")
		return
	}

	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(req.Body)
		if err != nil {
			c.String(http.StatusBadRequest, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}

	count, _ := hs.ic.FalconPush(body)
	c.JSON(http.StatusOK, Dto{Msg: "success", Data: &TransferResp{Msg: "ok", Total: count}})
	return
}
