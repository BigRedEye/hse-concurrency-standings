package config

import (
	log "github.com/sirupsen/logrus"

	"github.com/spf13/viper"
)

type Config struct {
	GoogleCredentialsPath string `mapstructure:"google_credentials_path"`
	GoogleSpreadsheetId   string `mapstructure:"google_spreadsheet_id"`
	GitLabToken           string `mapstructure:"gitlab_token"`
	GitLabGroup           string `mapstructure:"gitlab_group"`
	GitLabLabel           string `mapstructure:"gitlab_label"`
}

func LoadConfig() (*Config, error) {
	viper.BindEnv("GOOGLE_CREDENTIALS_PATH")
	viper.BindEnv("GOOGLE_SPREADSHEET_ID")
	viper.BindEnv("GITLAB_TOKEN")
	viper.BindEnv("GITLAB_GROUP")
	viper.BindEnv("GITLAB_LABEL")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Warn("Config file not found")
		} else {
			return nil, err
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
