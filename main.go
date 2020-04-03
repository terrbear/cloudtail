package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

var sess = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))

var cloudwatch = cloudwatchlogs.New(sess)

// DEBUG is just used internally for debugging
var DEBUG = true

func debug(message ...interface{}) {
	if DEBUG {
		log.Println()
		log.Println(message...)
		log.Println()
	}
}

type logPayload struct {
	logStream string
	events    []*cloudwatchlogs.OutputLogEvent
}

var logChannel chan logPayload = make(chan logPayload)

func printLogs() {
	for {
		payload := <-logChannel
		if len(payload.events) == 0 {
			continue
		}

		fmt.Println()
		fmt.Println("==>", payload.logStream, "<==")
		for _, event := range payload.events {
			fmt.Println(*event.Message)
		}
	}
}

func logFetch(logGroupName string, logStreamName string, token string) string {
	input := &cloudwatchlogs.GetLogEventsInput{
		Limit:         aws.Int64(10),
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
		NextToken:     aws.String(token),
		StartFromHead: aws.Bool(true),
	}

	output, err := cloudwatch.GetLogEvents(input)
	if err != nil {
		log.Fatal(err)
	}

	debug(output)

	logChannel <- logPayload{
		events:    output.Events,
		logStream: logStreamName,
	}

	if *output.NextForwardToken == token {
		debug("sleeping")
		time.Sleep(5 * time.Second)
	}

	return *output.NextForwardToken
}

func initialLogFetch(logGroupName string, logStreamName string) string {
	input := &cloudwatchlogs.GetLogEventsInput{
		Limit:         aws.Int64(10),
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
	}

	output, err := cloudwatch.GetLogEvents(input)
	if err != nil {
		log.Fatal(err)
	}

	debug(output)

	logChannel <- logPayload{
		events:    output.Events,
		logStream: logStreamName,
	}

	return *output.NextForwardToken
}

var logStreams map[string]bool = map[string]bool{}

func fetchLogStreams(logGroup string) {
	// just looking at the last 50 - because this polls if you have more recent events they'll bubble up but i never have more than 50 active log streams
	input := &cloudwatchlogs.DescribeLogStreamsInput{
		Descending:   aws.Bool(true),
		LogGroupName: aws.String(logGroup),
		OrderBy:      aws.String("LastEventTime"),
	}

	output, err := cloudwatch.DescribeLogStreams(input)
	if err != nil {
		log.Fatal(err)
	}

	dayAgo := time.Now().Add(-24*time.Hour).Unix() * 1000
	for _, stream := range output.LogStreams {
		if *stream.LastEventTimestamp >= dayAgo && !logStreams[*stream.LogStreamName] {
			logStreams[*stream.LogStreamName] = true
			go watchLogStream(logGroup, *stream.LogStreamName)
			// debug(stream)
		}
	}
	//debug(output)
}

func watchLogGroup(logGroup string) {
	for {
		fetchLogStreams(logGroup)
		time.Sleep(time.Minute)
	}
}

func watchLogStream(logGroup string, logStream string) {
	token := initialLogFetch(logGroup, logStream)
	for {
		token = logFetch(logGroup, logStream, token)
	}
}

func main() {
	logGroup := flag.String("log", "", "log group")
	logStream := flag.String("stream", "", "log stream")

	flag.Parse()

	if *logGroup == "" {
		flag.PrintDefaults()
		log.Fatal("must pass a log group, -log=xxx")
	}

	go printLogs()

	if *logStream == "" {
		watchLogGroup(*logGroup)
	} else {
		watchLogStream(*logGroup, *logStream)
	}
}
