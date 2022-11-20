package main

import (
	"os"

	"github.com/LiShangAn/real-time-data-pipeline/kapi"
	"github.com/LiShangAn/real-time-data-pipeline/util"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal().Err(err).Msg("cannot load config")
	}

	if config.Environment == "development" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	// init consumer
	reader, err := kapi.NewReader(config.ZookeeperURL, "car", "test1")
	if err != nil {
		log.Fatal().Err(err).Msg("error consumer group")
	}
	defer reader.Close()

	// run consumer
	reader.Consume()
}
