package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/app"
)

var (
	configPath = flag.String("config", "./configs/config.yaml", "path to config file")
	version    = "dev"
)

func main() {
	flag.Parse()

	fmt.Printf("pm-crypto-insurance-bot v%s\n", version)
	fmt.Printf("config path: %s\n", *configPath)

	application, err := app.New(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize application: %v\n", err)
		os.Exit(1)
	}

	if err := application.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "application error: %v\n", err)
		application.Shutdown()
		os.Exit(1)
	}

	application.Shutdown()
	fmt.Println("application exited cleanly")
	os.Exit(0)
}
