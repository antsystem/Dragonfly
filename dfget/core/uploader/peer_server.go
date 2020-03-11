/*
 * Copyright The Dragonfly Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uploader

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/core/helper"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/limitreader"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"github.com/dragonflyoss/Dragonfly/version"
	apitypes "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/constants"
	"github.com/dragonflyoss/Dragonfly/dfget/types"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// newPeerServer returns a new P2PServer.
func newPeerServer(cfg *config.Config, port int) *peerServer {
	s := &peerServer{
		cfg:      cfg,
		finished: make(chan struct{}),
		host:     cfg.RV.LocalIP,
		port:     port,
		api:      api.NewSupernodeAPI(),
	}

	s.syncTaskContainer = &taskContainer{
		syncTaskMap: sync.Map{},
		ps: 	s,
	}

	logrus.Infof("peer server config: %v, supernodes: %v", cfg, cfg.Nodes)

	s.reload()
	r := s.initRouter()
	s.Server = &http.Server{
		Addr:    net.JoinHostPort(s.host, strconv.Itoa(port)),
		Handler: r,
	}

	return s
}

// ----------------------------------------------------------------------------
// peerServer structure

// peerServer offers file-block to other clients.
type peerServer struct {
	cfg *config.Config

	// Finished indicates whether the peer server is shutdown
	finished chan struct{}

	// server related fields
	host string
	port int
	*http.Server

	api         api.SupernodeAPI
	rateLimiter *ratelimiter.RateLimiter

	// totalLimitRate is the total network bandwidth shared by tasks on the same host
	totalLimitRate int

	// syncTaskContainer stores the meta name of tasks on the host
	syncTaskContainer *taskContainer
}

type taskContainer struct {
	syncTaskMap sync.Map
	ps		*peerServer
}

func (t *taskContainer) Store(taskFileName string, tc *taskConfig) {
	t.syncTaskMap.Store(taskFileName, tc)
	t.syncToLocalFs(taskFileName, tc)
}

func (t *taskContainer) Load(taskFileName string) (interface{}, bool) {
	return t.syncTaskMap.Load(taskFileName)
}

func (t *taskContainer) Range(f func(key, value interface{}) bool) {
	t.syncTaskMap.Range(f)
}

func (t *taskContainer) Delete(taskFileName string) {
	t.syncTaskMap.Delete(taskFileName)
	fp := t.ps.taskMetaFilePath(taskFileName)
	fpBak := t.ps.taskMetaFileBakPath(taskFileName)

	os.Remove(fp)
	os.Remove(fpBak)
}

func (t *taskContainer) syncToLocalFs(key string, tc *taskConfig) {
	fpBak := t.ps.taskMetaFileBakPath(key)
	data, err := json.Marshal(tc)
	if err != nil {
		logrus.Warnf("failed to sync task %s to local fs: %v", key, err)
		return
	}

	err = ioutil.WriteFile(fpBak, data, 0664)
	if err != nil {
		logrus.Warnf("failed to sync task %s to local fs: %v", key, err)
		return
	}

	fp := t.ps.taskMetaFilePath(key)
	err = os.Rename(fpBak, fp)
	if err != nil {
		logrus.Warnf("failed to sync task %s to local fs: %v", key, err)
	}
}

// taskConfig refers to some name about peer task.
type taskConfig struct {
	TaskID     string		`json:"taskID"`
	RateLimit  int			`json:"rateLimit"`
	Cid        string		`json:"cid"`
	DataDir    string		`json:"dataDir"`
	SuperNode  string		`json:"superNode"`
	Finished   bool			`json:"finished"`
	AccessTime time.Time	`json:"accessTime"`
	Other      *api.FinishTaskOther `json:"other"`
	cache		*cacheBuffer	`json:"-"`
}

// uploadParam refers to all params needed in the handler of upload.
type uploadParam struct {
	padSize int64
	start   int64
	length  int64

	pieceSize int64
	pieceNum  int64
}

const(
	taskMetaDir = "task-meta"
)

// reload the resource which read from local file system
func (ps *peerServer) reload() {
	var(
		localTaskConfig = map[string]*taskConfig{}
		err error
	)

	taskMetaPath := filepath.Join(ps.cfg.RV.MetaPath, taskMetaDir)
	err = os.MkdirAll(taskMetaPath, 0744)
	if err != nil {
		panic(fmt.Sprintf("failed to init dir %s", taskMetaPath))
	}

	localTaskConfig, err = ps.readTaskInfoFromDir(filepath.Join(ps.cfg.RV.MetaPath, taskMetaDir))
	if err != nil {
		logrus.Warnf("failed to read task from local: %v", err)
		return
	}

	logrus.Infof("try to reload task file")
	ps.initLocalTask(localTaskConfig)
	ps.registerTaskToSuperNode()
}

func (ps *peerServer) registerTaskToSuperNode() {
	ps.syncTaskContainer.Range(func(key, value interface{}) bool {
		taskFileName := key.(string)
		tc := value.(*taskConfig)
		ps.reportResource(taskFileName, tc)
		return true
	})
}

func (ps *peerServer) reportResource(taskFileName string, tc *taskConfig) {
	req := &types.RegisterRequest{
		RawURL: tc.Other.RawURL,
		TaskURL: tc.Other.TaskURL,
		Cid: ps.cfg.RV.Cid,
		IP: ps.cfg.RV.LocalIP,
		Port: ps.cfg.RV.PeerPort,
		Path: taskFileName,
		Md5: ps.cfg.Md5,
		Identifier: ps.cfg.Identifier,
		Headers: tc.Other.Headers,
		Dfdaemon: ps.cfg.DFDaemon,
		Insecure: ps.cfg.Insecure,
		TaskId: tc.TaskID,
		FileLength: tc.Other.FileLength,
	}

	for _,node := range ps.cfg.Nodes {
		resp, err := ps.api.ReportResource(node, req)
		if err == nil && resp.Code == constants.Success {
			logrus.Infof("success to report resource %v to supernode", req)
			break
		}else{
			logrus.Errorf("failed to report resource %v tp supernode, resp: %v, err: %v", req, resp, err)
		}
	}
}

func (ps *peerServer) readTaskInfoFromDir(dir string) (map[string]*taskConfig, error) {
	result := map[string]*taskConfig{}

	fin, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, fi := range fin {
		taskFileName := ""
		isBak := false

		if strings.HasSuffix(fi.Name(), ".meta") {
			taskFileName = strings.TrimSuffix(fi.Name(), ".meta")
		}else if strings.HasSuffix(fi.Name(), ".meta.bak") {
			taskFileName = strings.TrimSuffix(fi.Name(), ".meta.bak")
			isBak = true
		}else{
			continue
		}

		fp  := filepath.Join(dir, fi.Name())
		meta, err := ioutil.ReadFile(fp)
		if err != nil {
			logrus.Errorf("failed to read meta data of task %s: %v", fi.Name(), err)
			continue
		}

		tc := &taskConfig{}

		err = json.Unmarshal(meta, tc)
		if err != nil {
			logrus.Errorf("failed to read meta data of task %s: %v", fi.Name(), err)
			continue
		}

		if !isBak {
			result[taskFileName] = tc
		}else{
			if _, exist := result[taskFileName]; !exist {
				result[taskFileName] = tc
			}
		}
	}

	return result, nil
}

func (ps *peerServer) initLocalTask(m map[string]*taskConfig) {
	for taskFileName, tc := range m {
		tc.AccessTime = time.Now()

		taskPath := helper.GetServiceFile(taskFileName, tc.DataDir)

		_, err := os.Stat(taskPath)
		if err != nil {
			logrus.Warnf("failed to stat taskFile %s for task %s: %v", taskPath, tc.TaskID, err)
			continue
		}

		tc.cache = newCacheBuffer()
		ps.syncTaskContainer.Store(taskFileName, tc)
		ps.syncCache(taskFileName)
		// todo: notify the supernode to register task
	}
}

func (ps *peerServer) taskMetaFilePath(taskFileName string) string {
	return filepath.Join(ps.cfg.RV.MetaPath, taskMetaDir, fmt.Sprintf("%s.meta", taskFileName))
}

func (ps *peerServer) taskMetaFileBakPath(taskFileName string) string {
	return filepath.Join(ps.cfg.RV.MetaPath, taskMetaDir, fmt.Sprintf("%s.meta.bak", taskFileName))
}

// ----------------------------------------------------------------------------
// reload method of peerServer

func (ps *peerServer) initRouter() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc(config.PeerHTTPPathPrefix+"{commonFile:.*}", ps.uploadHandler).Methods("GET")
	r.HandleFunc(config.LocalHTTPPathRate+"{commonFile:.*}", ps.parseRateHandler).Methods("GET")
	r.HandleFunc(config.LocalHTTPPathCheck+"{commonFile:.*}", ps.checkHandler).Methods("GET")
	r.HandleFunc(config.LocalHTTPPathClient+"finish", ps.oneFinishHandler).Methods("GET")
	r.HandleFunc(config.LocalHTTPPing, ps.pingHandler).Methods("GET")
	r.HandleFunc(config.LocalHTTPFETCH, ps.fetchHandler).Methods("GET")
	return r
}

// ----------------------------------------------------------------------------
// peerServer handlers

// uploadHandler uses to upload a task file when other peers download from it.
func (ps *peerServer) uploadHandler(w http.ResponseWriter, r *http.Request) {
	sendAlive(ps.cfg)

	var (
		up   *uploadParam
		f    *os.File
		size int64
		err  error
	)

	taskFileName := mux.Vars(r)["commonFile"]
	rangeStr := r.Header.Get(config.StrRange)

	logrus.Debugf("upload file:%s to %s, req:%v", taskFileName, r.RemoteAddr, jsonStr(r.Header))

	// Step1: parse param
	if up, err = parseParams(rangeStr, r.Header.Get(config.StrPieceNum),
		r.Header.Get(config.StrPieceSize)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		logrus.Warnf("invalid param file:%s req:%v, %v", taskFileName, r.Header, err)
		return
	}

	// get task config
	v, ok := ps.syncTaskContainer.Load(taskFileName)
	if !ok {
		rangeErrorResponse(w, fmt.Errorf("failed to get taskPath: %s", taskFileName))
		logrus.Errorf("failed to open file:%s, %v", taskFileName, err)
		return
	}

	task := v.(*taskConfig)
	// sync access time
	task.AccessTime = time.Now()

	if task.cache.valid {
		// try to upload from cache
		size = task.cache.size
		if err = amendRange(size, true, up); err != nil {
			rangeErrorResponse(w, err)
			logrus.Errorf("failed to amend range of file %s: %v", taskFileName, err)
			return
		}

		if err := ps.uploadPieceFromCache(task.cache, w, up); err != nil {
			logrus.Errorf("failed to send range(%s) of file(%s): %v", rangeStr, taskFileName, err)
		}

		return
	}

	// Step2: get task file
	if f, size, err = ps.getTaskFile(taskFileName); err != nil {
		rangeErrorResponse(w, err)
		logrus.Errorf("failed to open file:%s, %v", taskFileName, err)
		return
	}
	defer f.Close()

	// Step3: amend range with piece meta data
	if err = amendRange(size, true, up); err != nil {
		rangeErrorResponse(w, err)
		logrus.Errorf("failed to amend range of file %s: %v", taskFileName, err)
		return
	}

	// Step4: send piece wrapped by meta data
	if err := ps.uploadPiece(f, w, up); err != nil {
		logrus.Errorf("failed to send range(%s) of file(%s): %v", rangeStr, taskFileName, err)
	}
}

func (ps *peerServer) uploadPieceFromCache(c *cacheBuffer, w http.ResponseWriter, up *uploadParam) (e error) {
	w.Header().Set(config.StrContentLength, strconv.FormatInt(up.length, 10))
	sendHeader(w, http.StatusPartialContent)

	readLen := up.length - up.padSize
	buf := make([]byte, 256*1024)

	if up.padSize > 0 {
		binary.BigEndian.PutUint32(buf, uint32((readLen)|(up.pieceSize)<<4))
		w.Write(buf[:config.PieceHeadSize])
		defer w.Write([]byte{config.PieceTailChar})
	}

	brd := bytes.NewReader(c.buf.Bytes())
	brd.Seek(up.start, 0)
	r := io.LimitReader(brd, readLen)
	if ps.rateLimiter != nil {
		lr := limitreader.NewLimitReaderWithLimiter(ps.rateLimiter, r, false)
		_, e = io.CopyBuffer(w, lr, buf)
	} else {
		_, e = io.CopyBuffer(w, r, buf)
	}

	return
}

func (ps *peerServer) parseRateHandler(w http.ResponseWriter, r *http.Request) {
	sendAlive(ps.cfg)

	// get params from request
	taskFileName := mux.Vars(r)["commonFile"]
	rateLimit := r.Header.Get(config.StrRateLimit)
	clientRate, err := strconv.Atoi(rateLimit)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err.Error())
		logrus.Errorf("failed to convert RateLimit %v, %v", rateLimit, err)
		return
	}
	sendSuccess(w)

	// update the RateLimit of commonFile
	if v, ok := ps.syncTaskContainer.Load(taskFileName); ok {
		param := v.(*taskConfig)
		param.RateLimit = clientRate
	}

	// no need to calculate rate when totalLimitRate less than or equals zero.
	if ps.totalLimitRate <= 0 {
		fmt.Fprint(w, rateLimit)
		return
	}

	clientRate = ps.calculateRateLimit(clientRate)

	fmt.Fprint(w, strconv.Itoa(clientRate))
}

// checkHandler is used to check the server status.
// TODO: Disassemble this function for too many things done.
func (ps *peerServer) checkHandler(w http.ResponseWriter, r *http.Request) {
	sendAlive(ps.cfg)
	sendSuccess(w)

	// handle totalLimit
	totalLimit, err := strconv.Atoi(r.Header.Get(config.StrTotalLimit))
	if err == nil && totalLimit > 0 {
		if ps.rateLimiter == nil {
			ps.rateLimiter = ratelimiter.NewRateLimiter(int64(totalLimit), 2)
		} else {
			ps.rateLimiter.SetRate(ratelimiter.TransRate(int64(totalLimit)))
		}
		ps.totalLimitRate = totalLimit
		logrus.Infof("update total limit to %d", totalLimit)
	}

	// get parameters
	taskFileName := mux.Vars(r)["commonFile"]
	dataDir := r.Header.Get(config.StrDataDir)

	param := &taskConfig{
		DataDir: dataDir,
	}
	ps.syncTaskContainer.Store(taskFileName, param)
	fmt.Fprintf(w, "%s@%s", taskFileName, version.DFGetVersion)
}

// oneFinishHandler is used to update the status of peer task.
func (ps *peerServer) oneFinishHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		sendHeader(w, http.StatusBadRequest)
		fmt.Fprint(w, err.Error())
		return
	}

	taskFileName := r.FormValue(config.StrTaskFileName)
	taskID := r.FormValue(config.StrTaskID)
	cid := r.FormValue(config.StrClientID)
	superNode := r.FormValue(config.StrSuperNode)
	other := r.FormValue(config.StrOther)
	otherData, err := base64.StdEncoding.DecodeString(other)
	if err != nil {
		sendHeader(w, http.StatusBadRequest)
		fmt.Fprintf(w, "invalid params, failed to decode other data: %v", err)
		return
	}

	otherSt := api.FinishTaskOther{}
	err = json.Unmarshal(otherData, &otherSt)
	if err != nil {
		sendHeader(w, http.StatusBadRequest)
		fmt.Fprintf(w, "invalid params, failed to decode other data: %v", err)
		return
	}

	if taskFileName == "" || taskID == "" || cid == "" {
		sendHeader(w, http.StatusBadRequest)
		fmt.Fprintf(w, "invalid params")
		return
	}

	if v, ok := ps.syncTaskContainer.Load(taskFileName); ok {
		task := v.(*taskConfig)
		task.TaskID = taskID
		task.RateLimit = 0
		task.Cid = cid
		task.SuperNode = superNode
		task.Finished = true
		task.AccessTime = time.Now()
		task.cache = newCacheBuffer()
		task.Other = &otherSt
	} else {
		ps.syncTaskContainer.Store(taskFileName, &taskConfig{
			TaskID:     taskID,
			Cid:        cid,
			DataDir:    ps.cfg.RV.SystemDataDir,
			SuperNode:  superNode,
			Finished:   true,
			AccessTime: time.Now(),
			cache:      newCacheBuffer(),
			Other:      &otherSt,
		})
	}
	go ps.syncCache(taskFileName)
	sendSuccess(w)
	fmt.Fprintf(w, "success")
}

func (ps *peerServer) syncCache(taskFileName string) {
	v, ok := ps.syncTaskContainer.Load(taskFileName)
	if !ok {
		return
	}

	task := v.(*taskConfig)
	f,size, err := ps.getTaskFile(taskFileName)
	if err != nil {
		logrus.Errorf("in syncCache %s, failed to getTaskFile: %v", taskFileName, err)
		return
	}

	defer f.Close()

	buf := &bytes.Buffer{}
	copySize, _ := io.CopyN(buf, f, size)
	if copySize != size {
		logrus.Errorf("failed to syncCache %s from file %s, expected size %d, but got %d",
			taskFileName, f.Name(), size, copySize)
		return
	}
	task.cache.buf = buf
	task.cache.size = size
	task.cache.valid = true

	logrus.Infof("success to sync cache %s", taskFileName)
}

func (ps *peerServer) pingHandler(w http.ResponseWriter, r *http.Request) {
	sendSuccess(w)
	fmt.Fprintf(w, "success")
}

// ----------------------------------------------------------------------------
// handler process

// getTaskFile finds the file and returns the File object.
func (ps *peerServer) getTaskFile(taskFileName string) (*os.File, int64, error) {
	errSize := int64(-1)

	v, ok := ps.syncTaskContainer.Load(taskFileName)
	if !ok {
		return nil, errSize, fmt.Errorf("failed to get taskPath: %s", taskFileName)
	}
	tc, ok := v.(*taskConfig)
	if !ok {
		return nil, errSize, fmt.Errorf("failed to assert: %s", taskFileName)
	}

	// update the AccessTime of taskFileName
	tc.AccessTime = time.Now()

	taskPath := helper.GetServiceFile(taskFileName, tc.DataDir)

	fileInfo, err := os.Stat(taskPath)
	if err != nil {
		return nil, errSize, err
	}

	taskFile, err := os.Open(taskPath)
	if err != nil {
		return nil, errSize, err
	}
	return taskFile, fileInfo.Size(), nil
}

func amendRange(size int64, needPad bool, up *uploadParam) error {
	up.padSize = 0
	if needPad {
		up.padSize = config.PieceMetaSize
		up.start -= up.pieceNum * up.padSize
	}

	// we must send an whole piece with both piece head and tail
	if up.length < up.padSize || up.start < 0 {
		return errortypes.ErrRangeNotSatisfiable
	}

	if up.start >= size && !needPad {
		return errortypes.ErrRangeNotSatisfiable
	}

	if up.start+up.length-up.padSize > size {
		up.length = size - up.start + up.padSize
		if size == 0 {
			up.length = up.padSize
		}
	}

	return nil
}

// parseParams validates the parameter range and parses it.
func parseParams(rangeVal, pieceNumStr, pieceSizeStr string) (*uploadParam, error) {
	var (
		err error
		up  = &uploadParam{}
	)

	if up.pieceNum, err = strconv.ParseInt(pieceNumStr, 10, 64); err != nil {
		return nil, err
	}

	if up.pieceSize, err = strconv.ParseInt(pieceSizeStr, 10, 64); err != nil {
		return nil, err
	}

	if strings.Count(rangeVal, "=") != 1 {
		return nil, fmt.Errorf("invalid range: %s", rangeVal)
	}
	rangeStr := strings.Split(rangeVal, "=")[1]

	if strings.Count(rangeStr, "-") != 1 {
		return nil, fmt.Errorf("invalid range: %s", rangeStr)
	}
	rangeArr := strings.Split(rangeStr, "-")
	if up.start, err = strconv.ParseInt(rangeArr[0], 10, 64); err != nil {
		return nil, err
	}

	var end int64
	if end, err = strconv.ParseInt(rangeArr[1], 10, 64); err != nil {
		return nil, err
	}

	if end <= up.start {
		return nil, fmt.Errorf("invalid range: %s", rangeStr)
	}
	up.length = end - up.start + 1
	return up, nil
}

// uploadPiece sends a piece of the file to the remote peer.
func (ps *peerServer) uploadPiece(f *os.File, w http.ResponseWriter, up *uploadParam) (e error) {
	w.Header().Set(config.StrContentLength, strconv.FormatInt(up.length, 10))
	sendHeader(w, http.StatusPartialContent)

	readLen := up.length - up.padSize
	buf := make([]byte, 256*1024)

	if up.padSize > 0 {
		binary.BigEndian.PutUint32(buf, uint32((readLen)|(up.pieceSize)<<4))
		w.Write(buf[:config.PieceHeadSize])
		defer w.Write([]byte{config.PieceTailChar})
	}

	f.Seek(up.start, 0)
	r := io.LimitReader(f, readLen)
	if ps.rateLimiter != nil {
		lr := limitreader.NewLimitReaderWithLimiter(ps.rateLimiter, r, false)
		_, e = io.CopyBuffer(w, lr, buf)
	} else {
		_, e = io.CopyBuffer(w, r, buf)
	}

	return
}

func (ps *peerServer) calculateRateLimit(clientRate int) int {
	total := 0

	// define a function that Range will call it sequentially
	// for each key and value present in the map
	f := func(key, value interface{}) bool {
		if task, ok := value.(*taskConfig); ok {
			if !task.Finished {
				total += task.RateLimit
			}
		}
		return true
	}
	ps.syncTaskContainer.Range(f)

	// calculate the rate limit again according to totalLimit
	if total > ps.totalLimitRate {
		return (clientRate*ps.totalLimitRate + total - 1) / total
	}
	return clientRate
}

// ----------------------------------------------------------------------------
// methods of peerServer

func (ps *peerServer) isFinished() bool {
	if ps.finished == nil {
		return true
	}

	select {
	case _, notClose := <-ps.finished:
		return !notClose
	default:
		return false
	}
}

func (ps *peerServer) setFinished() {
	if !ps.isFinished() {
		close(ps.finished)
	}
}

func (ps *peerServer) waitForShutdown() {
	if ps.finished == nil {
		return
	}
	for {
		select {
		case _, notClose := <-ps.finished:
			if !notClose {
				return
			}
		}
	}
}

func (ps *peerServer) shutdown() {
	// tell supernode this peer node is down and delete related files.
	ps.syncTaskContainer.Range(func(key, value interface{}) bool {
		task, ok := value.(*taskConfig)
		if ok && !task.Other.SpecReport {
			ps.api.ServiceDown(task.SuperNode, task.TaskID, task.Cid)
			serviceFile := helper.GetServiceFile(key.(string), task.DataDir)
			os.Remove(serviceFile)
			logrus.Infof("shutdown, remove task id:%s file:%s",
				task.TaskID, serviceFile)
		}
		return true
	})

	c, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	ps.Shutdown(c)
	cancel()
	updateServicePortInMeta(ps.cfg.RV.MetaPath, 0)
	logrus.Info("peer server is shutdown.")
	ps.setFinished()
}

func (ps *peerServer) deleteExpiredFile(path string, info os.FileInfo,
	expireTime time.Duration) bool {
	taskName := helper.GetTaskName(info.Name())
	//logrus.Infof("fileName: %s, taskName: %s", info.Name(), taskName)
	if v, ok := ps.syncTaskContainer.Load(taskName); ok {
		task, ok := v.(*taskConfig)
		if ok && !task.Finished {
			return false
		}

		var lastAccessTime = task.AccessTime
		// use the bigger of access time and modify time to
		// check whether the task is expired
		if task.AccessTime.Sub(info.ModTime()) < 0 {
			lastAccessTime = info.ModTime()
		}
		// if the last access time is expireTime ago
		if time.Since(lastAccessTime) > expireTime {
			// ignore the gc
			if ok {
				if !task.Other.SpecReport {
					ps.api.ServiceDown(task.SuperNode, task.TaskID, task.Cid)
				}else{
					ps.api.ReportResourceDeleted(task.SuperNode, task.TaskID, task.Cid)
				}
			}
			os.Remove(path)
			ps.syncTaskContainer.Delete(taskName)
			return true
		}
	} else {
		os.Remove(path)
		return true
	}
	return false
}

// ----------------------------------------------------------------------------
// helper functions

func sendSuccess(w http.ResponseWriter) {
	sendHeader(w, http.StatusOK)
}

func sendHeader(w http.ResponseWriter, code int) {
	w.Header().Set(config.StrContentType, ctype)
	w.WriteHeader(code)
}

func rangeErrorResponse(w http.ResponseWriter, err error) {
	if errortypes.IsRangeNotSatisfiable(err) {
		http.Error(w, config.RangeNotSatisfiableDesc, http.StatusRequestedRangeNotSatisfiable)
	} else if os.IsPermission(err) {
		http.Error(w, err.Error(), http.StatusForbidden)
	} else if os.IsNotExist(err) {
		http.Error(w, err.Error(), http.StatusNotFound)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func jsonStr(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// fetchHandler fetchs the local resource
func (ps *peerServer) fetchHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		sendHeader(w, http.StatusBadRequest)
		fmt.Fprint(w, err.Error())
		return
	}

	// todo: add filter ?
	result := []*apitypes.TaskFetchInfo{}

	ps.syncTaskContainer.Range(func(key, value interface{}) bool {
		taskFileName := key.(string)
		tc := value.(*taskConfig)

		taskInfo := &apitypes.TaskFetchInfo{
			Pieces: []*apitypes.PieceInfo{
				{
					Path: taskFileName,
				},
			},
			Task: &apitypes.TaskInfo{
				ID: 	tc.TaskID,
				TaskURL: tc.Other.TaskURL,
				RawURL: tc.Other.RawURL,
				PieceSize: int32(tc.Other.FileLength),
				PieceTotal: 1,
				FileLength: tc.Other.FileLength,
				HTTPFileLength: tc.Other.FileLength,
				// Headers: tc.Other.Headers, todo: parse header
				Md5: ps.cfg.Md5,
				Identifier: ps.cfg.Identifier,
			},
		}

		result = append(result, taskInfo)
		return true
	})

	resp := &types.FetchLocalTaskInfo{
		Tasks: result,
	}

	respData, err := json.Marshal(resp)
	if err != nil {
		sendHeader(w, http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to encode %v: %v", resp, err)
		return
	}

	sendHeader(w, http.StatusOK)
	w.Write(respData)
	return
}
