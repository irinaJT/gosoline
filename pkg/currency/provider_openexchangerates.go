package currency

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/http"
	"github.com/justtrackio/gosoline/pkg/log"
)

const (
	OpenExchangeRatesApiProviderName = "openexchangeratesapi"
	OpenExchangeRatesUrl             = "https://openexchangerates.org/api/"
)

type OpenExchangeRatesApiResponse struct {
	Timestamp int64              `json:"timestamp"`
	Base      string             `json:"base"`
	Rates     map[string]float64 `json:"rates"`
}

func newOpenExchangeRatesApiProvider(_ context.Context, logger log.Logger, http http.Client, settings ProviderSettings) Provider {
	if settings.ApiKey == "" {
		return nil
	}

	return NewOpenExchangeRatesApiProviderWithInterfaces(logger, http, settings)
}

func NewOpenExchangeRatesApiProviderWithInterfaces(logger log.Logger, http http.Client, settings ProviderSettings) Provider {
	return &openExchangeRatesApiProvider{logger, http, settings}
}

type openExchangeRatesApiProvider struct {
	logger   log.Logger
	http     http.Client
	settings ProviderSettings
}

func (f *openExchangeRatesApiProvider) Name() string {
	return OpenExchangeRatesApiProviderName
}

func (f *openExchangeRatesApiProvider) GetPriority() int {
	return int(f.settings.Priority)
}

func (f *openExchangeRatesApiProvider) FetchLatestRates(ctx context.Context) ([]Rate, error) {
	request := f.http.NewRequest().WithUrl(OpenExchangeRatesUrl + "latest.json?app_id=" + f.settings.ApiKey + "&base=EUR")
	response, err := f.http.Get(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("error requesting openexchangerates: %w", err)
	}

	if response.StatusCode != 200 {
		return nil, fmt.Errorf("error requesting openexchangerates: status code %d", response.StatusCode)
	}

	fxResp := OpenExchangeRatesApiResponse{}
	if err := json.Unmarshal(response.Body, &fxResp); err != nil {
		return nil, fmt.Errorf("error unmarshalling openexchangerates response: %w", err)
	}

	rates := make([]Rate, 0, len(fxResp.Rates))
	for currency, rate := range fxResp.Rates {
		rates = append(rates, Rate{Currency: currency, Rate: rate})
	}

	return rates, nil
}

func (f *openExchangeRatesApiProvider) FetchHistoricalRates(ctx context.Context, dates []time.Time) (map[time.Time][]Rate, error) {
	result := make(map[time.Time][]Rate)
	for _, d := range dates {
		url := OpenExchangeRatesUrl + "historical/" + d.Format(time.DateOnly) + ".json/app_id=" + f.settings.ApiKey + "&base=EUR"

		request := f.http.NewRequest().WithUrl(url)

		response, err := f.http.Get(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("error requesting openexchangerates historical rates for %s: %v", d.Format(time.DateOnly), err)
		}

		if response.StatusCode != 200 {
			return nil, fmt.Errorf("error requesting openexchangerates historical rates for %s: status code %d", d.Format(time.DateOnly), response.StatusCode)
		}

		var fxResp OpenExchangeRatesApiResponse
		if err := json.Unmarshal(response.Body, &fxResp); err != nil {
			return nil, fmt.Errorf("error unmarshalling openexchangerates historical rates for %s: %v", d.Format(time.DateOnly), err)
		}

		dayRates := make([]Rate, 0, len(fxResp.Rates))

		for currency, rate := range fxResp.Rates {
			dayRates = append(dayRates, Rate{Currency: currency, Rate: rate})
		}

		result[d] = dayRates
	}

	return result, nil
}
