package stopwatch

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"time"
)

type taskInfo struct {
	taskName    string
	taskElapsed int64
}

type StopWatch struct {
	id              string
	latestTaskName  string
	taskList        *list.List
	latestStartTime time.Time
	taskCnt         int
	totalElapsed    int64
}

func New(id string) *StopWatch {
	return &StopWatch{id: id, taskList: list.New()}
}

func (w *StopWatch) Start(taskName string) error {
	if taskName == "" {
		return errors.New("task name must not be empty")
	}

	if w.latestTaskName != "" {
		return fmt.Errorf("can not start new stopwatch, current task: %s is running", w.latestTaskName)
	}

	w.latestTaskName = taskName
	w.latestStartTime = time.Now()

	return nil
}

func (w *StopWatch) MustStart(taskName string) {
	err := w.Start(taskName)

	if err != nil {
		panic(err)
	}
}

func (w *StopWatch) Stop() error {
	if w.latestTaskName == "" {
		return errors.New("can not stop StopWatch: it's not running")
	}

	elapsed := time.Since(w.latestStartTime).Nanoseconds()

	w.totalElapsed += elapsed
	lastTask := taskInfo{taskName: w.latestTaskName, taskElapsed: elapsed}
	w.taskList.PushBack(lastTask)
	w.taskCnt++
	w.latestTaskName = ""

	return nil
}

func (w *StopWatch) MustStop() {
	if err := w.Stop(); err != nil {
		panic(err)
	}
}

func (w *StopWatch) ShortSummary() string {
	return fmt.Sprintf("StopWatch '"+w.id+"': running time (ms) = %d\n", w.totalElapsed/1000000)
}

func (w *StopWatch) PrettyPrint() string {
	var buf bytes.Buffer

	buf.WriteString(w.ShortSummary())

	buf.WriteString("-----------------------------------------\n")
	buf.WriteString("ms        %         Task name\n")
	buf.WriteString("-----------------------------------------\n")

	for e := w.taskList.Front(); e != nil; e = e.Next() {
		taskInfo := e.Value.(taskInfo)
		
		var elapsed int64
		if w.totalElapsed != 0 {
			elapsed = taskInfo.taskElapsed*100/w.totalElapsed
		}
		
		buf.WriteString(fmt.Sprintf("%-10d", taskInfo.taskElapsed/1000000))
		buf.WriteString(fmt.Sprintf("%-10s", fmt.Sprintf("%d%%", elapsed)))
		buf.WriteString(fmt.Sprintf("%s\n", taskInfo.taskName))
	}
	return buf.String()
}

func (w *StopWatch) Clear() {
	w.taskList = list.New()
	w.totalElapsed = 0
	w.latestTaskName = ""
	w.taskCnt = 0
	w.id = ""
}