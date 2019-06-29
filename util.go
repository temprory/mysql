package mysql

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
)

var (
	inited = false
)

const (
	maxStack  = 20
	separator = "---------------------------------------\n"
)

func handlePanic() interface{} {
	if err := recover(); err != nil {
		errstr := fmt.Sprintf("\n%sruntime error: %v\ntraceback:\n", separator, err)

		i := 2
		for {
			pc, file, line, ok := runtime.Caller(i)
			if !ok || i > maxStack {
				break
			}
			errstr += fmt.Sprintf("    stack: %d %v [file: %s] [func: %s] [line: %d]\n", i-1, ok, file, runtime.FuncForPC(pc).Name(), line)
			i++
		}
		errstr += separator

		logDebug(errstr)

		return err
	}
	return nil
}

func safe(cb func()) {
	defer handlePanic()
	cb()
}

func safeGo(cb func()) {
	go func() {
		defer handlePanic()
		cb()
	}()
}

func handleSignal(handler func(sig os.Signal)) {
	if !inited {
		inited = true
		chSignal := make(chan os.Signal, 1)
		//signal.Notify(chSignal, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
		signal.Notify(chSignal)
		for {
			if sig, ok := <-chSignal; ok {
				logDebug("Recv Signal: %v", sig)

				if handler != nil {
					handler(sig)
				}
			} else {
				return
			}
		}
	}
}

func hash(str string) uint64 {
	var h = uint64(0)

	for _, c := range str {
		h = 31*h + uint64(c)
	}
	return h
}
