package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	email := flag.String("email", "example.com", "A regex that determines is an email is one of us")
	glob := flag.String("files", "*main.log*", "A glob pattern for matching exim logfiles to eat")
	logFrequency := flag.Int("log", 200, "The number of lines to read per log message")
	outFileName := flag.String("out", "emails", "The resulting email file")
	level := flag.String("level", "info", "Log level is one of debug, info, warn, error, fatal, panic")
	pretty := flag.Bool("pretty", true, "Use pretty logging (slower)")
	flag.Parse()

	if *pretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	loglevel, err := zerolog.ParseLevel(*level)
	if err != nil {
		log.Fatal().Str("loglevel", *level).Err(err).Msg("Loglevel must be one of debug, info, warn, error, fatal, panic")
	}
	zerolog.SetGlobalLevel(loglevel)
	zerolog.TimeFieldFormat = ""

	log.Info().
		Str("email", *email).
		Str("files", *glob).
		Int("frequency", *logFrequency).
		Str("outfile", *outFileName).
		Str("level", *level).
		Bool("pretty", *pretty).
		Msg("Starting exim4 logfile cruncher")

	emailRegex, err := regexp.Compile(*email)
	if err != nil {
		log.Fatal().Err(err).Msg("Email regex did not compile")
	}

	fileNames, err := filepath.Glob(*glob)
	if err != nil {
		log.Fatal().Str("pattern", *glob).Err(err).Msg("Failed to get files by glob")
	}

	outFile, err := os.Create(*outFileName)
	defer outFile.Close()
	if err != nil {
		log.Fatal().Str("name", *outFileName).Err(err).Msg("Failed to open output file")
	}

	emails := make(map[string]map[string]bool)
	lineMatch := regexp.MustCompile(`.+ <= (?P<from>\S+) .+ for (?P<to>\S+)`)
	lineCount := 0
	matchCount := 0
	ignoreCount := 0

FileLoop:
	for _, fileName := range fileNames {
		inFile, err := os.Open(fileName)
		defer inFile.Close()
		if err != nil {
			log.Error().Str("name", fileName).Err(err).Msg("Could not open file")
		}

		var reader *bufio.Reader
		if filepath.Ext(fileName) == ".gz" {
			gzReader, err := gzip.NewReader(inFile)
			defer gzReader.Close()
			if err != nil {
				log.Error().Str("name", fileName).Err(err).Msg("Could not read gzipped file")
			}
			reader = bufio.NewReader(gzReader)
		} else {
			reader = bufio.NewReader(inFile)
		}

		log.Info().Str("name", fileName).Msg("Reading file")
		logLineCount := *logFrequency
		for {
			if logLineCount <= 0 {
				log.Info().Int("lines", lineCount).Int("matched", matchCount).Int("ignored", ignoreCount).Msg("Crunching progress")
			}

			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Error().Str("name", fileName).Err(err).Msg("Could not read file")
					continue FileLoop
				}
			}

			matches := lineMatch.FindStringSubmatch(line)
			if matches != nil {
				to := matches[1]
				from := matches[2]
				toIsMatch := emailRegex.MatchString(to)
				fromIsMatch := emailRegex.MatchString(from)
				if toIsMatch && fromIsMatch {
					ignoreCount++
					continue
				}

				matchCount++
				if toIsMatch {
					val, ok := emails[to]
					if ok {
						val[from] = true
					} else {
						emails[to] = map[string]bool{from: true}
					}
				} else {
					val, ok := emails[from]
					if ok {
						val[to] = true
					} else {
						emails[from] = map[string]bool{to: true}
					}
				}
			}

			lineCount++
			logLineCount--
		}

		log.Debug().Str("file", fileName).Msg("Finished reading file")
	}

	log.Info().Int("count", matchCount).Msg("Writing emails to file")
	writer := bufio.NewWriter(outFile)
	for us, theirEmails := range emails {
		writer.WriteString(us)
		for them := range theirEmails {
			writer.WriteByte(',')
			writer.WriteString(them)
		}

		writer.WriteByte('\n')
		writer.Flush()
		log.Debug().Str("for", us).Msg("Finished emails")
	}

	log.Info().Int("lines", lineCount).Int("matched", matchCount).Int("ignored", ignoreCount).Msg("Finished crunching logfiles")
}
