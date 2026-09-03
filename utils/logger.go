package utils

import (
	"log/slog"
	"os"
	"strings"
)

func getLogLevelFromEnv() slog.Level {
    levelStr := os.Getenv("LOG_LEVEL")
    switch strings.ToLower(levelStr) {
    case "debug":
        return slog.LevelDebug
    case "info":
        return slog.LevelInfo
    case "warn":
        return slog.LevelWarn
    case "error":
        return slog.LevelError
    default:
        return slog.LevelInfo

    }
}


var Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	AddSource: true,
	Level: getLogLevelFromEnv(),

}))
