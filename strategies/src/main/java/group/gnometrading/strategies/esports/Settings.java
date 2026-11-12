package group.gnometrading.strategies.esports;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * The maker's arguments as configured, by their config names; null where the config leaves one out. Defaults are
 * applied by {@link EsportsPmMaker}.
 */
@JsonIgnoreProperties(ignoreUnknown = false)
record Settings(
        @JsonProperty("legs") List<VenueLegs> legs,
        @JsonProperty("maker_venue") String makerVenue,
        @JsonProperty("quote_size") Double quoteSize,
        @JsonProperty("depth_band") Double depthBand,
        @JsonProperty("requote_size_change") Double requoteSizeChange,
        @JsonProperty("calm_seconds") Double calmSeconds,
        @JsonProperty("calm_move") Double calmMove,
        @JsonProperty("quote_guard") Double quoteGuard,
        @JsonProperty("min_edge") Double minEdge,
        @JsonProperty("max_locked_usd") Double maxLockedUsd,
        @JsonProperty("completion_slippage") Double completionSlippage,
        @JsonProperty("completion_retry_seconds") Double completionRetrySeconds,
        @JsonProperty("cross_completions") Boolean crossCompletions,
        @JsonProperty("band_from_min_size") Boolean bandFromMinSize,
        @JsonProperty("sell_rival_venues") List<String> sellRivalVenues,
        @JsonProperty("book_depth") Integer bookDepth,
        @JsonProperty("processing_time_ns") Long processingTimeNs,
        @JsonProperty("league") String league,
        @JsonProperty("subject_id") Long subjectId,
        @JsonProperty("polymarket_slug") String polymarketSlug,
        @JsonProperty("kalshi_event_ticker") String kalshiEventTicker,
        @JsonProperty("scheduled_start") String scheduledStart,
        @JsonProperty("never_live_hours") Double neverLiveHours,
        @JsonProperty("stale_seconds") Double staleSeconds,
        @JsonProperty("ended_settle_seconds") Double endedSettleSeconds,
        @JsonProperty("ended_max_seconds") Double endedMaxSeconds,
        @JsonProperty("game_states") List<List<Object>> gameStates) {}
