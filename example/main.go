package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fsufitch/waitloop"
)

var loop = waitloop.NewCustom(&waitloop.LoopOptions{TTL: 15 * time.Second})

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		if len(strings.Fields(strings.TrimSpace(line))) < 2 {
			fmt.Println("need at least an action and a name")
			continue
		}

		go lineReceived(line)
	}

	// EOF, terminate loop
	fmt.Println("terminating loop and waiting one second for completions")
	loop.Terminate()
	<-time.After(1 * time.Second)
}

func lineReceived(line string) {
	switch {
	case strings.HasPrefix(line, "hello "):
		name := strings.TrimSpace(line[6:])
		hello(name)
	case strings.HasPrefix(line, "goodbye "):
		name := strings.TrimSpace(line[8:])
		loop.Send(waitloop.Event{Key: name})
	default:
		fmt.Println("start your message with 'hello ' or 'goodbye '")
	}
}

func hello(name string) {
	name = strings.TrimSpace(name)
	fmt.Printf("received hello for name `%s`; waiting for `goodbye %s`\n", name, name)
	event := <-loop.Wait(name)
	switch {
	case event.Error == waitloop.ErrTimedOut:
		fmt.Printf("timed out waiting for `goodbye %s`\n", name)
	case event.Error != nil:
		fmt.Printf("error waiting for `goodbye %s`: %v\n", name, event.Error)
	default:
		fmt.Printf("received goodbye for %s\n", name)
	}
}
