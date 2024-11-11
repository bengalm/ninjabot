package order

import (
	"context"
	"fmt"
	"github.com/bengalm/ninjabot/model"
	"github.com/bengalm/ninjabot/service"
)

type DataFeed struct {
	Data chan model.Order
	Err  chan error
}

type FeedConsumer func(order model.Order)

type Feed struct {
	exchange              service.Exchange
	OrderFeeds            map[string]*DataFeed
	SubscriptionsBySymbol map[string][]Subscription
}

type Subscription struct {
	onlyNewOrder bool
	consumer     FeedConsumer
}

func NewOrderFeed(e service.Exchange) *Feed {
	return &Feed{
		OrderFeeds:            make(map[string]*DataFeed),
		SubscriptionsBySymbol: make(map[string][]Subscription),
		exchange:              e,
	}
}

func (d *Feed) Subscribe(pair string, consumer FeedConsumer, onlyNewOrder bool) {
	if _, ok := d.OrderFeeds[pair]; !ok {
		d.OrderFeeds[pair] = &DataFeed{
			Data: make(chan model.Order),
			Err:  make(chan error),
		}
	}

	d.SubscriptionsBySymbol[pair] = append(d.SubscriptionsBySymbol[pair], Subscription{
		onlyNewOrder: onlyNewOrder,
		consumer:     consumer,
	})
}

func (d *Feed) Publish(order model.Order, _ bool) {
	if _, ok := d.OrderFeeds[order.Pair]; ok {
		d.OrderFeeds[order.Pair].Data <- order
	}
}

func (d *Feed) Start() {

	for pair := range d.OrderFeeds {
		go func(pair string, feed *DataFeed) {
			for order := range feed.Data {
				for _, subscription := range d.SubscriptionsBySymbol[pair] {
					subscription.consumer(order)
				}
			}
		}(pair, d.OrderFeeds[pair])
	}
}

func (d *Feed) SubWs(ctx context.Context) {
	go func() {
		subscription, errors := d.exchange.AccountSubscription(ctx)
		for {
			select {
			case err := <-errors:
				fmt.Printf("SubWs error: %v\n", err)
			case o := <-subscription:
				d.Publish(o, false)
			}
		}
	}()
}
