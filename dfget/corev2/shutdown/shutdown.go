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

package shutdown

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	lock       sync.Mutex
	shutdownFn []func()
)

func init() {
	go captureQuitSignal()
}

func RegisterShutdown(s func()) {
	lock.Lock()
	defer lock.Unlock()

	shutdownFn = append(shutdownFn, s)
}

func Shutdown() {
	for _, shutdown := range shutdownFn {
		shutdown()
	}
}

func captureQuitSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-c

	Shutdown()
}
