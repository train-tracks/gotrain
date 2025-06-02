package cmd

import (
	"encoding/json"
	"github.com/rijdendetreinen/gotrain/parsers"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rijdendetreinen/gotrain/api"
	"github.com/rijdendetreinen/gotrain/prometheus_interface"
	"github.com/rijdendetreinen/gotrain/receiver"
	"github.com/rijdendetreinen/gotrain/stores"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var serverCommand = &cobra.Command{
	Use:   "server",
	Short: "Start server",
	Long:  `Start the GoTrain server, which starts receiving new data and starts the REST API.`,
	Run: func(cmd *cobra.Command, args []string) {
		startServer(cmd)
	},
}

func init() {
	RootCmd.AddCommand(serverCommand)
}

var exitReceiverChannel = make(chan bool)
var exitRestAPI = make(chan bool)
var cleanupTicker *time.Ticker
var downtimeDetectorTicker *time.Ticker
var autoSaveTicker *time.Ticker

func startServer(cmd *cobra.Command) {
	initLogger(cmd)

	log.Info().Msgf("GoTrain %v starting", Version.VersionStringLong())

	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	signalChan := make(chan os.Signal, 1)
	shutdownFinished := make(chan struct{})

	signal.Notify(signalChan, os.Interrupt)
	signal.Notify(signalChan, syscall.SIGTERM)

	parsers.USZ = readUSZ()

	initStores()

	go func() {
		sig := <-signalChan
		log.Warn().Msgf("Received signal: %+v, shutting down", sig)
		signal.Reset()
		shutdown()
		close(shutdownFinished)
	}()

	receiver.ProcessStores = true
	receiver.ArchiveServices = false

	go receiver.ReceiveData(exitReceiverChannel)

	apiAddress := viper.GetString("api.address")
	go api.ServeAPI(apiAddress, exitRestAPI)

	if viper.GetBool("prometheus.enabled") {
		prometheus_interface.SetupPrometheus()
		prometheus_interface.StartPrometheusInterface()
	}

	setupCleanupScheduler()
	setupDowntimeDetector()
	setupAutoSave()

	<-shutdownFinished
	log.Warn().Msg("Exiting")
}

func setupCleanupScheduler() {
	cleanupTicker := time.NewTicker(1 * time.Minute)
	log.Debug().Msg("Cleanup scheduler set up")

	go func() {
		for {
			<-cleanupTicker.C
			stores.CleanUp()
		}
	}()
}

func setupDowntimeDetector() {
	downtimeDetectorTicker := time.NewTicker(20 * time.Second)
	log.Debug().Msg("Downtime detector set up")

	go func() {
		for {
			<-downtimeDetectorTicker.C
			stores.TakeMeasurements()
		}
	}()
}

func setupAutoSave() {
	autoSaveTicker := time.NewTicker(12 * time.Hour)
	log.Debug().Msg("Autosave set up")

	go func() {
		for {
			<-autoSaveTicker.C
			log.Info().Msg("Auto-saving stores")
			log.Info().Msgf("Current inventory: %d arrivals, %d departures, %d services",
				stores.Stores.ArrivalStore.GetNumberOfArrivals(),
				stores.Stores.DepartureStore.GetNumberOfDepartures(),
				stores.Stores.ServiceStore.GetNumberOfServices())
			stores.SaveStores()
		}
	}()
}

func initStores() {
	stores.InitializeStores()

	if viper.IsSet("stores.location") {
		stores.StoresDataDirectory = viper.GetString("stores.location")
	}

	if _, err := os.Stat(stores.StoresDataDirectory); os.IsNotExist(err) {
		log.Error().Str("directory", stores.StoresDataDirectory).Msg("Data directory does not exist; not loading stores")
	} else {
		log.Info().Str("directory", stores.StoresDataDirectory).Msg("Data directory initialized")

		log.Info().Msg("Reading saved store contents...")
		stores.LoadStores()
	}
}

func shutdown() {
	log.Warn().Msg("Shutting down")

	if cleanupTicker != nil {
		cleanupTicker.Stop()
	}

	if downtimeDetectorTicker != nil {
		downtimeDetectorTicker.Stop()
	}

	if autoSaveTicker != nil {
		autoSaveTicker.Stop()
	}

	exitRestAPI <- true
	exitReceiverChannel <- true

	<-exitRestAPI
	<-exitReceiverChannel

	log.Info().Msg("Saving store contents...")
	stores.SaveStores()
}

// readUSZ reads the exit sides from disk
func readUSZ() map[string]map[string]map[string]string {
	b, err := os.ReadFile("USZ_20240715.json")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read USZ_20240715.json")
	}

	var uszDocument map[string]map[string]map[string]string
	err = json.Unmarshal(b, &uszDocument)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to unmarshal USZ_20240715.json")
	}

	return uszDocument
}
