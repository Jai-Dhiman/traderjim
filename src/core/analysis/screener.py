from __future__ import annotations
"""Options screener for finding credit spread opportunities."""

from dataclasses import dataclass
from datetime import datetime

from core.analysis.greeks import days_to_expiry, years_to_expiry, calculate_greeks
from core.analysis.iv_rank import IVMetrics, is_elevated_iv
from core.broker.types import OptionContract, OptionsChain
from core.types import CreditSpread, Greeks, OptionContract as CoreOptionContract, SpreadType


@dataclass
class ScreenerConfig:
    """Configuration for the options screener."""
    # DTE range
    min_dte: int = 30
    max_dte: int = 45

    # Delta range for short strike
    min_delta: float = 0.20
    max_delta: float = 0.30

    # IV requirements
    min_iv_rank: float = 50.0

    # Minimum credit (as percentage of width)
    min_credit_pct: float = 0.25  # 25% of spread width

    # Spread width range
    min_width: float = 1.0
    max_width: float = 10.0

    # Liquidity filters
    min_open_interest: int = 100
    min_volume: int = 10
    max_bid_ask_spread_pct: float = 0.10  # 10% of mid price


@dataclass
class ScoredSpread:
    """A credit spread with a score for ranking."""
    spread: CreditSpread
    score: float
    iv_rank: float
    expected_value: float
    probability_otm: float


class OptionsScreener:
    """Screens options chains for credit spread opportunities."""

    # Target underlyings per PRD
    UNDERLYINGS = ["SPY", "QQQ", "IWM"]

    def __init__(self, config: ScreenerConfig | None = None):
        self.config = config or ScreenerConfig()

    def screen_chain(
        self,
        chain: OptionsChain,
        iv_metrics: IVMetrics,
    ) -> list[ScoredSpread]:
        """Screen an options chain for credit spread opportunities.

        Args:
            chain: Options chain data from broker
            iv_metrics: IV metrics for the underlying

        Returns:
            List of scored spreads, sorted by score descending
        """
        if not is_elevated_iv(iv_metrics.iv_rank, self.config.min_iv_rank):
            return []

        opportunities = []

        # Filter expirations to DTE range
        valid_expirations = [
            exp for exp in chain.expirations
            if self.config.min_dte <= days_to_expiry(exp) <= self.config.max_dte
        ]

        for expiration in valid_expirations:
            # Find bull put spreads (bullish/neutral)
            put_opportunities = self._find_bull_put_spreads(
                chain, expiration, iv_metrics
            )
            opportunities.extend(put_opportunities)

            # Find bear call spreads (bearish/neutral)
            call_opportunities = self._find_bear_call_spreads(
                chain, expiration, iv_metrics
            )
            opportunities.extend(call_opportunities)

        # Sort by score descending
        opportunities.sort(key=lambda x: x.score, reverse=True)

        return opportunities

    def _find_bull_put_spreads(
        self,
        chain: OptionsChain,
        expiration: str,
        iv_metrics: IVMetrics,
    ) -> list[ScoredSpread]:
        """Find bull put spread opportunities (sell higher put, buy lower put)."""
        puts = chain.get_puts(expiration)
        puts = self._filter_for_liquidity(puts)

        if len(puts) < 2:
            return []

        # Sort by strike descending
        puts.sort(key=lambda x: x.strike, reverse=True)

        opportunities = []
        tte = years_to_expiry(expiration)

        for i, short_put in enumerate(puts):
            # Check short strike delta
            short_delta = self._get_delta(
                short_put, chain.underlying_price, tte, iv_metrics.current_iv, "put"
            )
            if not (self.config.min_delta <= abs(short_delta) <= self.config.max_delta):
                continue

            # Find long put candidates (lower strikes)
            for long_put in puts[i + 1:]:
                width = short_put.strike - long_put.strike
                if not (self.config.min_width <= width <= self.config.max_width):
                    continue

                spread = self._build_spread(
                    chain.underlying,
                    SpreadType.BULL_PUT,
                    short_put,
                    long_put,
                    expiration,
                )

                if spread.credit <= 0:
                    continue

                # Check minimum credit
                credit_pct = spread.credit / width
                if credit_pct < self.config.min_credit_pct:
                    continue

                # Score the spread
                scored = self._score_spread(
                    spread, iv_metrics, abs(short_delta)
                )
                opportunities.append(scored)

        return opportunities

    def _find_bear_call_spreads(
        self,
        chain: OptionsChain,
        expiration: str,
        iv_metrics: IVMetrics,
    ) -> list[ScoredSpread]:
        """Find bear call spread opportunities (sell lower call, buy higher call)."""
        calls = chain.get_calls(expiration)
        calls = self._filter_for_liquidity(calls)

        if len(calls) < 2:
            return []

        # Sort by strike ascending
        calls.sort(key=lambda x: x.strike)

        opportunities = []
        tte = years_to_expiry(expiration)

        for i, short_call in enumerate(calls):
            # Check short strike delta
            short_delta = self._get_delta(
                short_call, chain.underlying_price, tte, iv_metrics.current_iv, "call"
            )
            if not (self.config.min_delta <= abs(short_delta) <= self.config.max_delta):
                continue

            # Find long call candidates (higher strikes)
            for long_call in calls[i + 1:]:
                width = long_call.strike - short_call.strike
                if not (self.config.min_width <= width <= self.config.max_width):
                    continue

                spread = self._build_spread(
                    chain.underlying,
                    SpreadType.BEAR_CALL,
                    short_call,
                    long_call,
                    expiration,
                )

                if spread.credit <= 0:
                    continue

                # Check minimum credit
                credit_pct = spread.credit / width
                if credit_pct < self.config.min_credit_pct:
                    continue

                # Score the spread
                scored = self._score_spread(
                    spread, iv_metrics, abs(short_delta)
                )
                opportunities.append(scored)

        return opportunities

    def _filter_for_liquidity(
        self, contracts: list[OptionContract]
    ) -> list[OptionContract]:
        """Filter contracts for minimum liquidity."""
        filtered = []
        for c in contracts:
            if c.open_interest < self.config.min_open_interest:
                continue
            if c.volume < self.config.min_volume:
                continue
            if c.bid <= 0 or c.ask <= 0:
                continue

            # Check bid-ask spread
            mid = (c.bid + c.ask) / 2
            spread_pct = (c.ask - c.bid) / mid if mid > 0 else 1.0
            if spread_pct > self.config.max_bid_ask_spread_pct:
                continue

            filtered.append(c)

        return filtered

    def _get_delta(
        self,
        contract: OptionContract,
        spot: float,
        tte: float,
        iv: float,
        option_type: str,
    ) -> float:
        """Get delta for a contract (from broker or calculated)."""
        if contract.delta is not None:
            return contract.delta

        # Calculate if not provided
        greeks = calculate_greeks(
            spot=spot,
            strike=contract.strike,
            time_to_expiry=tte,
            volatility=iv,
            option_type=option_type,
        )
        return greeks.delta

    def _build_spread(
        self,
        underlying: str,
        spread_type: SpreadType,
        short_contract: OptionContract,
        long_contract: OptionContract,
        expiration: str,
    ) -> CreditSpread:
        """Build a CreditSpread from broker contracts."""
        # Convert to core types
        short_core = CoreOptionContract(
            symbol=short_contract.symbol,
            underlying=underlying,
            expiration=expiration,
            strike=short_contract.strike,
            option_type=short_contract.option_type,
            bid=short_contract.bid,
            ask=short_contract.ask,
            last=short_contract.last,
            volume=short_contract.volume,
            open_interest=short_contract.open_interest,
            implied_volatility=short_contract.implied_volatility or 0.0,
            greeks=Greeks(
                delta=short_contract.delta or 0.0,
                gamma=short_contract.gamma or 0.0,
                theta=short_contract.theta or 0.0,
                vega=short_contract.vega or 0.0,
            ) if short_contract.delta else None,
        )

        long_core = CoreOptionContract(
            symbol=long_contract.symbol,
            underlying=underlying,
            expiration=expiration,
            strike=long_contract.strike,
            option_type=long_contract.option_type,
            bid=long_contract.bid,
            ask=long_contract.ask,
            last=long_contract.last,
            volume=long_contract.volume,
            open_interest=long_contract.open_interest,
            implied_volatility=long_contract.implied_volatility or 0.0,
            greeks=Greeks(
                delta=long_contract.delta or 0.0,
                gamma=long_contract.gamma or 0.0,
                theta=long_contract.theta or 0.0,
                vega=long_contract.vega or 0.0,
            ) if long_contract.delta else None,
        )

        return CreditSpread(
            underlying=underlying,
            spread_type=spread_type,
            short_strike=short_contract.strike,
            long_strike=long_contract.strike,
            expiration=expiration,
            short_contract=short_core,
            long_contract=long_core,
        )

    def _score_spread(
        self,
        spread: CreditSpread,
        iv_metrics: IVMetrics,
        short_delta: float,
    ) -> ScoredSpread:
        """Score a spread for ranking.

        Score factors:
        - Higher IV rank = better premium
        - Delta closer to 0.25 (sweet spot) = better probability
        - Higher credit/width ratio = better risk/reward
        - Expected value (credit * prob_win - max_loss * prob_loss)
        """
        # Probability of expiring OTM (rough estimate from delta)
        prob_otm = 1 - abs(short_delta)

        # Expected value per spread
        credit = spread.credit * 100  # Per contract
        max_loss = spread.max_loss
        expected_value = (credit * prob_otm) - (max_loss * (1 - prob_otm))

        # Score components (normalized)
        iv_score = iv_metrics.iv_rank / 100  # 0-1
        delta_score = 1 - abs(abs(short_delta) - 0.25) * 4  # Peak at 0.25
        credit_score = min(spread.credit / spread.width, 0.5) * 2  # 0-1
        ev_score = max(0, expected_value) / (spread.width * 100)  # Normalized by width

        # Weighted average
        score = (
            iv_score * 0.25
            + delta_score * 0.25
            + credit_score * 0.25
            + ev_score * 0.25
        )

        return ScoredSpread(
            spread=spread,
            score=score,
            iv_rank=iv_metrics.iv_rank,
            expected_value=expected_value,
            probability_otm=prob_otm,
        )
