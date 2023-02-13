import json
import threading
from typing import List
import math
import logging
import numpy as np

import pytz
from datetime import datetime, date

import requests
from betfairlightweight.resources import MarketBook
from flumine import BaseStrategy
from flumine.markets.market import Market
from flumine.order.order import BaseOrder, OrderStatus
from flumine.order.ordertype import LimitOrder, LimitOnCloseOrder
from flumine.order.trade import Trade
from flumine.utils import get_price, get_nearest_price

#
# context:
# - min_back_price
# - max_back_price
# - min_lay_price
# - max_lay_price
# - seconds_to_start (eg. -30.0: 30s after race supposed to start)
# - stake (default bet size)
# - margin: minimum Expected Value when placing bets (0.1 = 10%)
# - staking_strategy:
#   - 'offer': offer bets at current back/lay price (add to stack)
#   - 'take': take bets at offered back/lay price
#   - 'bsp': place bets at BSP
#
class ExampleStrategy(BaseStrategy):
    # Generate random probabilities
    def _generate_probabilities(self, market: Market, market_book: MarketBook):
        if market.market_id not in self.market_probabilities:
            probabilities = np.random.default_rng().uniform(0, 1, market_book.number_of_active_runners)
            probabilities = probabilities / probabilities.sum()
            self.market_probabilities[market.market_id] = probabilities
            logging.info(f'Generated random probabilities for maket={market.market_id}, probabilities={probabilities}')
        return self.market_probabilities[market.market_id]

    def add(self):
        # library added to framework
        logging.info("Added strategy 'ExampleStrategy' to framework")

        self.market_probabilities = {}
        
        self.seconds_to_start = int(self.context.get("seconds_to_start", 60))
        self.margin = float(self.context.get("margin", 0.1))
        self.min_back_price = float(self.context.get("min_back_price", 1))
        self.max_back_price = float(self.context.get("max_back_price", 150))
        self.min_lay_price = float(self.context.get("min_lay_price", 1))
        self.max_lay_price = float(self.context.get("max_lay_price", 150))

    def start(self):
        # subscribe to streams
        logging.info("Starting strategy 'ExampleStrategy'")

    # Return False if we don't want to process this market
    def check_market_book(self, market: Market, market_book: MarketBook):
        if market is None or market_book is None:
            logging.debug(f"Skip check_market_book due empty market_book market={market}")
            return False

        # Skip closed markets
        if market_book.status != "OPEN":
            logging.debug(f"Skip check_market_book status={market_book.status}")
            return False

        # Do not trade in-play
        if market_book.inplay is True:
            logging.debug(f"Skip check_market_book market_book.inplay={market_book.inplay}")
            return False

        # Skip races with less than n runners
        if market_book.number_of_active_runners < 2:
            logging.debug(f"Skip check_market_book min number_of_active_runners={market_book.number_of_active_runners}")
            return False

        # Skip races with more than n runners
        if market_book.number_of_active_runners > 8:
            logging.debug(f"Skip check_market_book max number_of_active_runners={market_book.number_of_active_runners}")
            return False

        # Skip market based on start time
        if market.seconds_to_start > self.seconds_to_start:
            logging.debug(f"Skip check_market_book before trading window market.seconds_to_start={market.seconds_to_start}, seconds_to_start={self.seconds_to_start}")
            return False

        return True

    # Actual strategy logic
    def process_market_book(self, market: Market, market_book: MarketBook):
        if market is None or market_book is None:
            logging.debug(f"Skip process_market_book due empty data market={market}, market_book={market_book}")
            return False

        logging.debug(f'process_market_book market_id={market.market_id}')

        # Generate probabilities
        race_probability = self._generate_probabilities(market, market_book)

        # Validate probabilities have correctly been generated
        if race_probability is None or len(race_probability) != market_book.number_of_active_runners:
            logging.warning(f"Cannot generate probabilities for market_id {market.market_id}")
            return False

        # Verify probabilities are valid for each runner
        runner_idx = 0
        for runner in market_book.runners:
            if runner.status != 'ACTIVE':
                continue

            # Get probability for this runner
            runner_probability = race_probability[runner_idx]
            if runner_probability is None or math.isnan(runner_probability) or runner_probability == 0:
                logging.warning(f"Invalid runner probability for market_id={market.market_id}, race={race}, runner={runner}, runner_probability={runner_probability}")
                return False

            runner_idx = runner_idx+1

        # Compare market odds and probabilities
        runner_idx = 0
        for runner in market_book.runners:
            if runner.status != 'ACTIVE':
                continue

            # Get probability for this runner
            runner_probability = race_probability[runner_idx]

            # Get best back/lay prices
            back_price = get_price(runner.ex.available_to_back, 0)
            lay_price = get_price(runner.ex.available_to_lay, 0)
            logging.debug(f"runner selection_id={runner.selection_id}, runner_probability={runner_probability}, best_back_price={back_price}, best_lay_price={lay_price}")

            trade = Trade(
                market_id=market_book.market_id,
                selection_id=runner.selection_id,
                handicap=runner.handicap,
                strategy=self,
            )

            # Place bets at BSP if no bets have already been placed
            if self.context['staking_strategy'] == 'bsp':
                return self._place_bsp_bets(market, runner, back_price, lay_price, runner_probability, trade)

            has_back_bets = False
            has_lay_bets = False

            # If we currently have unmatched bets on the exchange, update prices to stay in the best lay/back price stack
            selection_orders = market.blotter.strategy_selection_orders(self, runner.selection_id, runner.handicap)
            for order in selection_orders:
                # Back bets
                if order.side == 'BACK':
                    has_back_bets = True
                    if order.status == OrderStatus.EXECUTABLE:
                        proposed_price = self._get_back_price(runner_probability, lay_price, back_price)
                        if proposed_price is not None and proposed_price < order.order_type.price:
                            market.replace_order(order, proposed_price)
                # Lay bets
                if order.side == 'LAY':
                    has_lay_bets = True
                    if order.status == OrderStatus.EXECUTABLE:
                        proposed_price = self._get_lay_price(runner_probability, lay_price, back_price)
                        if proposed_price is not None and proposed_price > order.order_type.price:
                            market.replace_order(order, proposed_price)

            # Offer or Take prices
            # Back bets
            if not has_back_bets:
                proposed_price = self._get_back_price(runner_probability, lay_price, back_price)
                if proposed_price is not None:
                    order = trade.create_order(
                        side="BACK",
                        order_type=LimitOrder(proposed_price, self.context['stake'])
                    )
                    market.place_order(order)
                
            # Lay bets
            if not has_lay_bets:
                proposed_price = self._get_lay_price(runner_probability, lay_price, back_price)
                if proposed_price is not None:
                    order = trade.create_order(
                        side="LAY",
                        order_type=LimitOrder(proposed_price, self.context['stake'])
                    )
                    market.place_order(order)
            
            runner_idx = runner_idx+1

    def process_orders(self, market: Market, orders: List[BaseOrder]):
        if market is None or orders is None:
            return False

    # Executed when market is closed
    def process_closed_market(self, market: Market, market_book: MarketBook) -> None:
        market_back_pnl = 0
        market_lay_pnl = 0
        market_pnl = 0
        market_commission = 0
        
        # Get total profit/loss
        for runner in market_book.runners:
            selection_orders = market.blotter.strategy_selection_orders(self, runner.selection_id, runner.handicap)
            for order in selection_orders:
                if runner.status == 'WINNER':
                    if order.side == 'BACK':
                        market_back_pnl += order.size_matched * (order.order_type.price - 1)
                    if order.side == 'LAY':
                        market_lay_pnl -= order.size_matched * (order.order_type.price - 1)
                if runner.status == 'LOSER':
                    if order.side == 'BACK':
                        market_back_pnl -= order.size_matched
                    if order.side == 'LAY':
                        market_lay_pnl += order.size_matched

        market_pnl = market_back_pnl + market_lay_pnl
        
        # Calculate commission on profitable markets
        if market_pnl > 0:
            # Get market base rate
            market_base_rate = market_book.market_definition.market_base_rate
            if market_base_rate in (None, 0, ''):
                market_base_rate = 5
            market_commission = market_pnl * market_base_rate / 100 if market_pnl > 0 else 0
            market_pnl -= market_commission

        logging.info(f'TOTAL market_id={market.market_id}\tback={round(market_back_pnl, 2)}\tlay={round(market_lay_pnl, 2)}\tmarket_pnl={round(market_pnl, 2)},\tmarket_commission={round(market_commission, 2)}')


    # Calculate our back price based on staking strategy:
    # - take: Get best back price available
    # - offer: Get best lay price available (to be added to lay stack)
    def _get_back_price(self, runner_probability, lay_price, back_price):
        # Our probability is greater than best lay probability => back at best lay price
        # eg:
        # - back_price = 4
        # - lay_price = 5
        # - back_probability = 1/back_price = 0.25
        # - lay_probability = 1/lay_price = 0.20
        # - runner_probability = 0.30
        # => back at lay_price 5

        # No price provided
        if lay_price is None or back_price is None:
            return None

        # Defined price based on our staking strategy
        price = None
        if self.context['staking_strategy'] == 'take':
            price = back_price
        elif self.context['staking_strategy'] == 'offer':
            price = lay_price

        # Verify proposed price is within Expected Value range
        runner_price_ev = (1 / runner_probability) * (1 + self.margin)
        if runner_price_ev < price:
            return None

        # Check min/max back price
        if self.min_back_price > price:
            return None
        if self.max_back_price < price:
            return None

        return price

    # Calculate our lay price based on staking strategy:
    # - take: Get best lay price available
    # - offer: Get best back price available (to be added to back stack
    def _get_lay_price(self, runner_probability, lay_price, back_price):
        # Our probability is lower than best back probability => lay at best back price
        # eg:
        # - back_price = 4
        # - lay_price = 5
        # - back_probability = 1/back_price = 0.25
        # - lay_probability = 1/lay_price = 0.20
        # - runner_probability = 0.15
        # => lay at lay_price 4

        # No price provided
        if lay_price is None or back_price is None:
            return None

        # Defined price based on our staking strategy
        price = None
        if self.context['staking_strategy'] == 'take':
            price = lay_price
        elif self.context['staking_strategy'] == 'offer':
            price = back_price

        # Verify proposed price is within Expected Value range
        runner_price_ev = (1 / runner_probability) / (1 + self.margin)
        if runner_price_ev > price:
            return None

        # Check min/max back price
        if self.min_lay_price > price:
            return None
        if self.max_lay_price < price:
            return None

        return price

    def _place_bsp_bets(self, market, runner, back_price, lay_price, runner_probability, trade):
        # Skip if we have already placed bets on this runner
        runner_context = self.get_runner_context(market.market_id, runner.selection_id, runner.handicap)
        if runner_context.trade_count == 0:
            return None

        # Skip if there are no available prices
        if back_price is None or lay_price is None:
            return None

        # TODO: improve projected BSP calculation based on amount available
        projected_bsp = (back_price + lay_price) / 2

        limit_back_price = (1 / runner_probability) * (1 + self.margin)
        limit_back_price_tick = get_nearest_price(limit_back_price)
        limit_lay_price = (1 / runner_probability) / (1 + self.margin)
        limit_lay_price_tick = get_nearest_price(limit_lay_price)
        limit_lay_liability = round((projected_bsp - 1) * self.context['stake'], 2)

        # Min liability for lay bets is 30AUD
        if limit_lay_liability < 30:
            limit_lay_liability = 30

        # Back bet
        if self.min_back_price <= limit_back_price_tick <= self.max_back_price:
            order = trade.create_order(
                side="BACK",
                order_type=LimitOnCloseOrder(liability=self.context['stake'], price=limit_back_price_tick)
            )
            market.place_order(order)

        # Lay bet
        if self.min_lay_price <= limit_lay_price_tick <= self.max_lay_price:
            order = trade.create_order(
                side="LAY",
                order_type=LimitOnCloseOrder(liability=limit_lay_liability, price=limit_lay_price_tick)
            )
            market.place_order(order)
