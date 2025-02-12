package main

import (
	"github.com/justtrackio/gosoline/examples/getting_started/api/definer"
	"github.com/justtrackio/gosoline/pkg/apiserver"
	"github.com/justtrackio/gosoline/pkg/application"
	"github.com/justtrackio/gosoline/pkg/currency"
)

func main() {
	application.Run(
		application.WithConfigFile("config.dist.yml", "yml"),
		application.WithKernelSettingsFromConfig,
		application.WithLoggerHandlersFromConfig,

		application.WithModuleFactory("api", apiserver.New(definer.ApiDefiner)),
		application.WithModuleFactory("currency", currency.NewCurrencyModule()),
	)
}
