package logger

import (
	"fmt"
	"io"
	"os"
	"time"
)

type LogLevel int

type LogTopic string

const (
	LL_ALL LogLevel = iota
	LL_TRACE
	LL_DEBUG
	LL_INFO
	LL_WARN
	LL_ERROR
	LL_FATAL
)

var ll_desc = []string{"ALL", "TRACE", "DEBUG", "INFO ", "WARN ", "ERROR", "FATAL"}

// var defaultLogger = NewLogger(LL_TRACE, os.Stdout, "")
// var defaultLogger = NewLogger(LL_DEBUG, os.Stdout, "")
var defaultLogger = NewLogger(LL_INFO, os.Stdout, "")
// var defaultLogger = NewLogger(LL_WARN, os.Stdout, "")

const (
	LT_Client     LogTopic = "CLNT"
	LT_Commit     LogTopic = "CMIT"
	LT_Drop       LogTopic = "DROP"
	LT_Leader     LogTopic = "LEAD"
	LT_Candidate  LogTopic = "CAND"
	LT_Log        LogTopic = "LOG1"
	LT_Log2       LogTopic = "LOG2"
	LT_Persist    LogTopic = "PERS"
	LT_Snap       LogTopic = "SNAP"
	LT_Term       LogTopic = "TERM"
	LT_Test       LogTopic = "TEST"
	LT_Timer      LogTopic = "TIMR"
	LT_Trace      LogTopic = "TRCE"
	LT_Vote       LogTopic = "VOTE"
	LT_APPLIER    LogTopic = "APLY"
	LT_CLERK      LogTopic = "CLRK"
	LT_SERVER     LogTopic = "SRVR"
	LT_CtrlServer LogTopic = "CTRL_SR"
	LT_CtrlClerk  LogTopic = "CTRL_CK"
	LT_Rebalance  LogTopic = "REBA"
  LT_Shard      LogTopic = "SHRD"
)

type Logger struct {
	level      LogLevel
	out        io.Writer
	prefix     string
	debugStart time.Time
}

func NewLogger(level LogLevel, out io.Writer, prefix string) *Logger {
	return &Logger{
		level:      level,
		out:        out,
		prefix:     prefix,
		debugStart: time.Now(),
	}
}

func (l *Logger) SetLogLevel(level LogLevel) {
	l.level = level
}

func (l *Logger) buildPrefix(level LogLevel, topic LogTopic) string {
	time := time.Since(l.debugStart).Milliseconds()
	// time /= 100
	prefix := fmt.Sprintf("[%v] %06d ", ll_desc[level], time)
	if l.prefix != "" {
		prefix = fmt.Sprintf("%v%v ", prefix, l.prefix)
	}
	if topic != "" {
		prefix = fmt.Sprintf("%v%v  ", prefix, topic)
	} else {
		prefix = fmt.Sprintf("%v      ", prefix)
	}
	return prefix
}

func (l *Logger) log(level LogLevel, topic LogTopic, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	prefix := l.buildPrefix(level, topic)

	msg := fmt.Sprintf(format, args...)
	msg = prefix + msg
	fmt.Print(msg)
}

func (l *Logger) Trace(topic LogTopic, format string, args ...interface{}) {
	l.log(LL_TRACE, topic, format, args...)
}

func (l *Logger) Debug(topic LogTopic, format string, args ...interface{}) {
	l.log(LL_DEBUG, topic, format, args...)
}

func (l *Logger) Info(topic LogTopic, format string, args ...interface{}) {
	l.log(LL_INFO, topic, format, args...)
}

func (l *Logger) Warn(topic LogTopic, format string, args ...interface{}) {
	l.log(LL_WARN, topic, format, args...)
}

func (l *Logger) Error(topic LogTopic, format string, args ...interface{}) {
	l.log(LL_ERROR, topic, format, args...)
	panic("ERROR")
}

func (l *Logger) Fatal(topic LogTopic, format string, args ...interface{}) {
	defer os.Exit(1)
	l.log(LL_FATAL, topic, format, args...)
}

func (l *Logger) Traceln(topic LogTopic, message interface{}) {
	l.SimpleLog(LL_TRACE, topic, message)
}

func (l *Logger) Debugln(topic LogTopic, message interface{}) {
	l.SimpleLog(LL_DEBUG, topic, message)
}

func (l *Logger) Infoln(topic LogTopic, message interface{}) {
	l.SimpleLog(LL_INFO, topic, message)
}

func (l *Logger) Warnln(topic LogTopic, message interface{}) {
	l.SimpleLog(LL_WARN, topic, message)
}

func (l *Logger) Errorln(topic LogTopic, message interface{}) {
	l.SimpleLog(LL_ERROR, topic, message)
	panic("ERROR")
}

func (l *Logger) Fatalln(topic LogTopic, message interface{}) {
	l.SimpleLog(LL_FATAL, topic, message)
	os.Exit(1)
}

func Trace(topic LogTopic, format string, args ...interface{}) {
	defaultLogger.log(LL_TRACE, topic, format, args...)
}

func Info(topic LogTopic, format string, args ...interface{}) {
	defaultLogger.log(LL_INFO, topic, format, args...)
}

func Debug(topic LogTopic, format string, args ...interface{}) {
	defaultLogger.log(LL_DEBUG, topic, format, args...)
}

func Warn(topic LogTopic, format string, args ...interface{}) {
	defaultLogger.log(LL_WARN, topic, format, args...)
}

func Error(topic LogTopic, format string, args ...interface{}) {
	defaultLogger.log(LL_ERROR, topic, format, args...)
}

func Fatal(topic LogTopic, format string, args ...interface{}) {
	defaultLogger.log(LL_FATAL, topic, format, args...)
}

func (l *Logger) SimpleLog(level LogLevel, topic LogTopic, message interface{}) {
	l.log(level, topic, "%v\n", message)
}

// 全局函数用于简单日志输出
func Traceln(topic LogTopic, message interface{}) {
	defaultLogger.SimpleLog(LL_TRACE, topic, message)
}

func Debugln(topic LogTopic, message interface{}) {
	defaultLogger.SimpleLog(LL_DEBUG, topic, message)
}

func Infoln(topic LogTopic, message interface{}) {
	defaultLogger.SimpleLog(LL_INFO, topic, message)
}

func Warnln(topic LogTopic, message interface{}) {
	defaultLogger.SimpleLog(LL_WARN, topic, message)
}

func Errorln(topic LogTopic, message interface{}) {
	defaultLogger.SimpleLog(LL_ERROR, topic, message)
}

func Fatalln(topic LogTopic, message interface{}) {
	defaultLogger.SimpleLog(LL_FATAL, topic, message)
	os.Exit(1)
}
