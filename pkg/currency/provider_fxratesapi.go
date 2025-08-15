package currency

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/http"
	"github.com/justtrackio/gosoline/pkg/log"
)

const FxRatesApiProviderName = "fxratesapi"

type FxRatesApiResponse struct {
	Success   bool               `json:"success"`
	Timestamp int64              `json:"timestamp"`
	Date      string             `json:"date"`
	Base      string             `json:"base"`
	Rates     map[string]float64 `json:"rates"`
}

func FxRatesApiProviderOption() ProviderOptions {
	return func(ctx context.Context, logger log.Logger, http http.Client, settings ProviderSettings) (Provider, error) {
		if settings.ApiKey == "" {
			return nil, fmt.Errorf("no fxratesapi provider api key set")
		}

		logger.Info("using fxratesapi as currency provider")

		return &fxRatesApiProvider{logger, http, &settings}, nil
	}
}

type fxRatesApiProvider struct {
	logger   log.Logger
	http     http.Client
	settings *ProviderSettings
}

func (f *fxRatesApiProvider) Name() string {
	return FxRatesApiProviderName
}

func (f *fxRatesApiProvider) GetPriority() int {
	return int(f.settings.Priority)
}

func (f *fxRatesApiProvider) FetchLatestRates(ctx context.Context) ([]Rate, error) {
	request := f.http.NewRequest().WithUrl(ExchangeRateFxRatesUrl + "latest?access_key=" + f.settings.ApiKey)
	response, err := f.http.Get(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("error requesting fixrates: %w", err)
	}

	fxResp := FxRatesApiResponse{}
	if err := json.Unmarshal(response.Body, &fxResp); err != nil {
		return nil, fmt.Errorf("error unmarshalling fixrates response: %w", err)
	}
	if !fxResp.Success {
		return nil, fmt.Errorf("fixrates response not successful")
	}

	rates := make([]Rate, 0, len(fxResp.Rates))
	for currency, rate := range fxResp.Rates {
		rates = append(rates, Rate{Currency: currency, Rate: rate})
	}

	return rates, nil
}

func (f *fxRatesApiProvider) FetchHistoricalRates(ctx context.Context, dates []time.Time) (map[time.Time][]Rate, error) {
	result := make(map[time.Time][]Rate)
	for _, d := range dates {
		url := ExchangeRateFxRatesUrl + "historical?access_key=" + f.settings.ApiKey + "&base=EUR&date=" + d.Format("2006-01-02")

		request := f.http.NewRequest().WithUrl(url)

		response, err := f.http.Get(ctx, request)
		if err != nil {
			f.logger.Warn("error requesting fxratesapi historical rates for %s: %v", d.Format("2006-01-02"), err)

			continue
		}

		var fxResp FxRatesApiResponse
		if err := json.Unmarshal(response.Body, &fxResp); err != nil {
			f.logger.Warn("error unmarshalling fxratesapi historical rates for %s: %v", d.Format("2006-01-02"), err)

			continue
		}

		if !fxResp.Success {
			f.logger.Warn("fxratesapi response not successful for %s", d.Format("2006-01-02"))

			continue
		}

		dayRates := make([]Rate, 0, len(fxResp.Rates))

		for currency, rate := range fxResp.Rates {
			dayRates = append(dayRates, Rate{Currency: currency, Rate: rate})
		}

		result[d] = dayRates
	}

	return result, nil
}
