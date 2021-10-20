package job

import (
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type MachineryLogger struct{}

// Print sends to logger.Info
func (m *MachineryLogger) Print(args ...interface{}) {
	logger.JobLogger.Info(args...)
}

// Printf sends to logger.Infof
func (m *MachineryLogger) Printf(format string, args ...interface{}) {
	logger.JobLogger.Infof(format, args...)
}

// Println sends to logger.Info
func (m *MachineryLogger) Println(args ...interface{}) {
	logger.JobLogger.Info(args...)
}

// Fatal sends to logger.Fatal
func (m *MachineryLogger) Fatal(args ...interface{}) {
	logger.JobLogger.Fatal(args...)
}

// Fatalf sends to logger.Fatalf
func (m *MachineryLogger) Fatalf(format string, args ...interface{}) {
	logger.JobLogger.Fatalf(format, args...)
}

// Fatalln sends to logger.Fatal
func (m *MachineryLogger) Fatalln(args ...interface{}) {
	logger.JobLogger.Fatal(args...)
}

// Panic sends to logger.Panic
func (m *MachineryLogger) Panic(args ...interface{}) {
	logger.JobLogger.Panic(args...)
}

// Panicf sends to logger.Panic
func (m *MachineryLogger) Panicf(format string, args ...interface{}) {
	logger.JobLogger.Panic(args...)
}

// Panicln sends to logrus.Panic
func (m *MachineryLogger) Panicln(args ...interface{}) {
	logger.JobLogger.Panic(args...)
}
