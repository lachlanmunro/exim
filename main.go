package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/rs/zerolog/log"
)

func main() {
	email := flag.String("email", "test.com", "A regex that determines is an email is one of us")
	glob := flag.String("glob", "*main.log", "A glob pattern for matching exim logfiles to eat")
	logFrequency := flag.Int("log", 100, "The number of lines to read per log message")
	outFileName := flag.String("out", "emails", "The resulting email file")
	flag.Parse()
	log.Debug().Str("email", *email).Str("glob", *glob).Time("time", time.Now()).Msg("spinning up")

	emailRegex, err := regexp.Compile(*email)
	if err != nil {
		log.Fatal().Err(err).Msg("email regex did not compile")
	}

	fileNames, err := filepath.Glob(*glob)
	if err != nil {
		log.Fatal().Str("glob", *glob).Err(err).Msg("failed to get files")
	}

	outFile, err := os.Create(*outFileName)
	defer outFile.Close()
	if err != nil {
		log.Fatal().Str("file", *outFileName).Err(err).Msg("failed to open output file")
	}

	emails := make(map[string]map[string]bool)
	lineMatch := regexp.MustCompile(`.+ <= (?P<from>\S+) .+ for (?P<to>\S+)`)
	lineCount := 0
	matchCount := 0
	ignoreCount := 0
	for _, fileName := range fileNames {
		inFile, err := os.Open(fileName)
		defer inFile.Close()
		if err != nil {
			log.Error().Str("file", fileName).Err(err).Msg("could not open")
		}
		log.Debug().Str("file", fileName).Time("time", time.Now()).Msg("reading file")
		reader := bufio.NewReader(inFile)
		logLineCount := *logFrequency
		for {
			if logLineCount <= 0 {
				log.Debug().Int("lines", lineCount).Int("matched", matchCount).Int("ignored", ignoreCount).Time("time", time.Now()).Msg("have processed")
			}
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Error().Str("file", fileName).Err(err).Msg("could not read")
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
		log.Debug().Str("file", fileName).Time("time", time.Now()).Msg("finished reading file")
	}
	writer := bufio.NewWriter(outFile)
	for us, theirEmails := range emails {
		writer.WriteString(us)
		for them := range theirEmails {
			writer.WriteByte(',')
			writer.WriteString(them)
		}
		writer.WriteByte('\n')
		writer.Flush()
		log.Debug().Str("for", us).Msg("finished emails")
	}
	log.Debug().Int("lines", lineCount).Int("matched", matchCount).Int("ignored", ignoreCount).Time("time", time.Now()).Msg("done")
}
