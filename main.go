package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	ignoreRegex    *regexp.Regexp
	emailRegex     *regexp.Regexp
	emails         = make(map[string]map[string]bool)
	writeLock      = sync.Mutex{}
	sem            chan bool
	lineMatch      = regexp.MustCompile(`.+ <= (?P<from>\S+) .+ for (?P<to>\S+)`)
	lineCount      = 0
	matchCount     = 0
	ignoreCount    = 0
	fromCount      = 0
	remainingFiles = 0
	startTime      = time.Now()
	logLineCount   = 1
	logFrequency   = 1
)

func main() {
	email := flag.String("email", ".*", "A regex that determines is an email should be selected to group against")
	ignore := flag.String("ignore", "^$", "A regex that determines if a to email should be ignored")
	glob := flag.String("files", "*main.log*", "A glob pattern for matching exim logfiles to eat")
	logFreq := flag.Int("log", 1000000, "The number of lines to read per log message")
	outFileName := flag.String("out", "emails", "The resulting email file")
	level := flag.String("level", "info", "Log level is one of debug, info, warn, error, fatal, panic")
	pretty := flag.Bool("pretty", true, "Use pretty logging (slower)")
	threads := flag.Int("threads", 500, "The number of lines to read per log message")
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
		Int("frequency", *logFreq).
		Str("outfile", *outFileName).
		Str("level", *level).
		Str("ignore", *ignore).
		Bool("pretty", *pretty).
		Msg("Starting exim4 logfile cruncher")

	ignoreRegex, err = regexp.Compile(*ignore)
	if err != nil {
		log.Fatal().Err(err).Msg("Ignore regex did not compile")
	}

	emailRegex, err = regexp.Compile(*email)
	if err != nil {
		log.Fatal().Err(err).Msg("Email regex did not compile")
	}

	fileNames, err := filepath.Glob(*glob)
	if err != nil {
		log.Fatal().Str("pattern", *glob).Err(err).Msg("Failed to get files by glob")
	}
	remainingFiles = len(fileNames)

	outFile, err := os.Create(*outFileName)
	defer outFile.Close()
	if err != nil {
		log.Fatal().Str("name", *outFileName).Err(err).Msg("Failed to open output file")
	}

	logFrequency = *logFreq
	logLineCount = logFrequency
	sem = make(chan bool, *threads)
	for _, fileName := range fileNames {
		sem <- true
		go processFile(fileName)
	}
	for i := 0; i < cap(sem); i++ {
		sem <- true
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

	log.Info().
		Int("lines", lineCount).
		Int("matched", matchCount).
		Int("ignored", ignoreCount).
		Int("from", fromCount).
		Dur("elapsed", time.Since(startTime)).
		Msg("Finished crunching logfiles")
}

const letterDiff = 'A' - 'a'

func toLower(r rune) rune {
	if 'A' <= r && r <= 'Z' {
		return r - letterDiff
	}
	return r
}

func processFile(fileName string) {
	defer func() { <-sem }()
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

	log.Info().Str("name", fileName).Int("remaining", remainingFiles).Msg("Reading file")
	for {
		if logLineCount <= 0 {
			logLineCount = logFrequency
			log.Info().
				Int("lines", lineCount).
				Int("matched", matchCount).
				Int("ignored", ignoreCount).
				Int("from", fromCount).
				Msg("Crunching progress")
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Error().Str("name", fileName).Err(err).Msg("Could not read file")
				return
			}
		}

		matches := lineMatch.FindSubmatch(line)
		if matches != nil {
			from := matches[1]
			if !emailRegex.Match(from) {
				ignoreCount++
				continue
			}

			to := matches[2]
			if ignore := ignoreRegex.Match(to); ignore {
				ignoreCount++
				continue
			}

			fromAsString := string(bytes.Map(toLower, from))
			toAsString := string(bytes.Map(toLower, to))
			writeLock.Lock()
			val, ok := emails[fromAsString]
			if ok {
				val[toAsString] = true
			} else {
				fromCount++
				emails[fromAsString] = map[string]bool{toAsString: true}
			}
			writeLock.Unlock()
			matchCount++
		}

		lineCount++
		logLineCount--
	}

	remainingFiles--
	log.Debug().Str("file", fileName).Dur("elapsed", time.Since(startTime)).Msg("Finished reading file")
}
