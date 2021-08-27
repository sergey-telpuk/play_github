package cli

import (
	"context"
	"example.com/m/internal/csv"
	"example.com/m/internal/service/analytics"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"time"
)

var (
	cfgFile string
	// Root command
	rootCmd = &cobra.Command{
		TraverseChildren: true,
	}
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	cobra.OnInitialize(initConfig)
}

// New root command
func New() *cobra.Command {
	command1 := &cobra.Command{
		Use:   "sort-users-by-prs-and-commits-pushed",
		Short: "Top 10 active users sorted by amount of PRs created and commits pushed",
		Run:   cmdPrintTop10ActiveUsersSortedByAmountOfPRsCreatedAndCommitsPushed,
	}
	command1.Flags().String("actors", "", "the path for file")
	_ = viper.BindPFlag("actors", command1.Flags().Lookup("actors"))
	command1.Flags().String("events", "", "the path for file")
	_ = viper.BindPFlag("events", command1.Flags().Lookup("events"))
	rootCmd.AddCommand(command1)

	command2 := &cobra.Command{
		Use:   "sort-repos-by-commits-pushed",
		Short: "Top 10 repositories sorted by amount of commits pushed",
		Run:   cmdPrintTop10repositoriesByAmountOfCommitsPushed,
	}
	command2.Flags().String("repos", "", "the path for file")
	_ = viper.BindPFlag("repos", command2.Flags().Lookup("repos"))
	command2.Flags().String("events", "", "the path for file")
	_ = viper.BindPFlag("events", command2.Flags().Lookup("events"))
	rootCmd.AddCommand(command2)

	command3 := &cobra.Command{
		Use:   "sort-repos-by-watch-events",
		Short: "Top 10 repositories sorted by amount of watch events",
		Run:   cmdPrintTop10repositoriesSortedByAmountOfWatchEvents,
	}
	command3.Flags().String("repos", "", "the path for file")
	_ = viper.BindPFlag("repos", command3.Flags().Lookup("repos"))
	command3.Flags().String("events", "", "the path for file")
	_ = viper.BindPFlag("events", command3.Flags().Lookup("events"))
	rootCmd.AddCommand(command3)

	init := &cobra.Command{
		Use:   "init",
		Short: "",
		Run: func(cmd *cobra.Command, args []string) {
			cmdPrintTop10ActiveUsersSortedByAmountOfPRsCreatedAndCommitsPushed(cmd, args)
			cmdPrintTop10repositoriesByAmountOfCommitsPushed(cmd, args)
			cmdPrintTop10repositoriesSortedByAmountOfWatchEvents(cmd, args)
		},
	}
	init.Flags().String("actors", "", "the path for file")
	_ = viper.BindPFlag("actors", init.Flags().Lookup("actors"))
	init.Flags().String("repos", "", "the path for file")
	_ = viper.BindPFlag("repos", init.Flags().Lookup("repos"))
	init.Flags().String("events", "", "the path for file")
	_ = viper.BindPFlag("events", init.Flags().Lookup("events"))

	rootCmd.AddCommand(init)

	return rootCmd
}

func cmdPrintTop10ActiveUsersSortedByAmountOfPRsCreatedAndCommitsPushed(cmd *cobra.Command, args []string) {
	service, closeFiles, err := initAnalyticsService()
	defer func() {
		err = closeFiles()
		if err != nil {
			log.Error(fmt.Sprintf("failed to print err: %v", err))
			os.Exit(1)
		}
	}()
	if err != nil {
		log.Error(fmt.Sprintf("failed to  init the AnalyticsService err: %v", err))
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = service.PrintTop10ActiveUsersSortedByAmountOfPRsCreatedAndCommitsPushed(ctx)
	if err != nil {
		log.Error(fmt.Sprintf("failed to print err: %v", err))
		os.Exit(1)
	}
}

func cmdPrintTop10repositoriesByAmountOfCommitsPushed(cmd *cobra.Command, args []string) {
	service, closeFiles, err := initAnalyticsService()
	defer closeFiles()

	if err != nil {
		log.Error(fmt.Sprintf("failed to  init the AnalyticsService err: %v", err))
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = service.PrintTop10repositoriesByAmountOfCommitsPushed(ctx)
	if err != nil {
		log.Error(fmt.Sprintf("failed to print err: %v", err))
		os.Exit(1)
	}
}
func cmdPrintTop10repositoriesSortedByAmountOfWatchEvents(cmd *cobra.Command, args []string) {
	service, closeFiles, err := initAnalyticsService()
	defer func() {
		err := closeFiles()
		if err != nil {
			log.Error(fmt.Sprintf("failed to  close the files err: %v", err))
			os.Exit(1)
		}
	}()

	if err != nil {
		log.Error(fmt.Sprintf("failed to  init the AnalyticsService err: %v", err))
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = service.PrintTop10repositoriesSortedByAmountOfWatchEvents(ctx)
	if err != nil {
		log.Error(fmt.Sprintf("failed to print err: %v", err))
		os.Exit(1)
	}
}

func initAnalyticsService() (analytics.Service, func() error, error) {
	pathActors := viper.GetString("actors")
	pathEvents := viper.GetString("events")
	pathRepos := viper.GetString("repos")

	//events
	eventsFile, err := os.OpenFile(pathEvents, os.O_RDONLY, 0600)
	if err != nil {
		log.Error(fmt.Sprintf("Unable to read input err: %v", err))
		os.Exit(1)
	}

	eventsReader, err := csv.NewReader(eventsFile)
	if err != nil {
		return analytics.Service{}, nil, err
	}
	// users
	usersFile, err := os.OpenFile(pathActors, os.O_RDONLY, 0600)
	if err != nil {
		return analytics.Service{}, nil, err
	}

	usersReader, err := csv.NewReader(usersFile)
	if err != nil {
		return analytics.Service{}, nil, err
	}

	// repos
	reposFile, err := os.OpenFile(pathRepos, os.O_RDONLY, 0600)
	if err != nil {
		return analytics.Service{}, nil, err
	}

	reposReader, err := csv.NewReader(reposFile)
	if err != nil {
		return analytics.Service{}, nil, err
	}

	closeFiles := func() error {
		err := eventsFile.Close()
		if err != nil {
			return err
		}
		err = reposFile.Close()
		if err != nil {
			return err
		}
		err = usersFile.Close()
		if err != nil {
			return err
		}
		return nil
	}

	analyticsService := analytics.NewAnalyticsService(
		eventsReader,
		usersReader,
		reposReader,
	)

	return analyticsService, closeFiles, nil
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".cobra")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
