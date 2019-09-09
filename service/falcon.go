package main

import (
	"compress/gzip"
	"io/ioutil"
	"net/http"

	json "github.com/json-iterator/go"
)

func  (hs *HttpService)FalconRegister(mux *http.ServeMux) {
	mux.HandleFunc("/api/push", hs.push)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("5.1.1"))
	})

	mux.HandleFunc("/workdir", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("/home/"))
	})

	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		_, _ = w.Write([]byte(""))
	})

	mux.HandleFunc("/config/reload", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	//mux.HandleFunc("/debug/connpool/", func(w http.ResponseWriter, r *http.Request) {
	//	_, _ = w.Write([]byte(""))
	//})

	// counter
	mux.HandleFunc("/counter/all", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		_, _ = w.Write([]byte(""))
	})

	// TO BE DISCARDed
	mux.HandleFunc("/statistics/all", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		_, _ = w.Write([]byte(""))
	})

	// step
	mux.HandleFunc("/proc/step", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		_, _ = w.Write([]byte(""))
	})

	// trace
	mux.HandleFunc("/trace/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		_, _ = w.Write([]byte(""))
	})

	// filter
	mux.HandleFunc("/filter/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		_, _ = w.Write([]byte(""))
	})
}

func RenderJson(w http.ResponseWriter, v interface{}) {
	bs, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_, _ = w.Write(bs)
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



func (hs *HttpService)push(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if req.ContentLength == 0 {
		http.Error(w, "blank body", http.StatusBadRequest)
		return
	}

	if req.Method != "POST" {
		w.WriteHeader(405)
		_, _ = w.Write([]byte("method not allow."))
		return
	}

	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(req.Body)
		if err != nil {
			w.WriteHeader(400)
			_, _ = w.Write([]byte("unable to decode gzip body"))
			return
		}
		defer b.Close()
		body = b
	}

	p, err := ioutil.ReadAll(body)
	if err != nil {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	count, _ := hs.ic.FalconPush(p)

	bs, _ := json.Marshal(Dto{Msg: "success", Data: &TransferResp{Msg:"ok", Total:count}})
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_, _ = w.Write(bs)
	return
}
