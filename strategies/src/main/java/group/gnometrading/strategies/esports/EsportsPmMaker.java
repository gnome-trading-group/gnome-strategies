package group.gnometrading.strategies.esports;

import group.gnometrading.SecurityMaster;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sequencer.metrics.MetricRecorder;
import group.gnometrading.sequencer.metrics.MetricStream;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.sm.fee.FeeComponent;
import group.gnometrading.sm.fee.FeeSchedule;
import group.gnometrading.sm.fee.FeeTimeline;
import group.gnometrading.strategies.StrategyAgent;
import group.gnometrading.utils.SampledEpochNanoClock;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.EpochNanoClock;

/**
 * Esports cross-venue arb, maker on Polymarket International: rest a bid on each team there, and when one fills buy
 * the other team at once on Kalshi or Polymarket US.
 *
 * <p>A pair is one contract paying $1 if team A wins and one paying $1 if team B wins, so a pair bought for less than
 * $1 after fees is locked whatever happens. Polymarket International holds marketable orders on these markets for
 * about a second while resting orders post at once, so resting there gets that second's protection. Each team's bid
 * is priced from what the other team costs to take elsewhere right now ({@link MakerQuote}), so a fill locks
 * {@code min_edge} per pair if the hedge is still there. Bids are post-only, re-priced on every book update, and
 * pulled while a fill is being hedged and once the session holds {@code max_locked_usd}.
 *
 * <p>A fill is hedged by buying the short team on the other venues ({@link Completion}) until both teams pay the
 * same. Each hedge order is limited at the cheapest route's cost plus {@code completion_slippage}, so it crosses
 * whatever the book still has up to that cap, and one that comes back unfilled is planned again on the next update
 * of a hedge venue's book. {@code cross_completions} off limits each at the level it was costed on, and
 * {@code completion_retry_seconds} adds a wait after an unfilled one, as the Python strategy did. A hedge the venue
 * rejected is never sent again unchanged. Hedging is never held back by the match state or the stake cap: an
 * unhedged position is the one risk the strategy exists not to hold.
 *
 * <p>Nothing is quoted before the match is live: a match never played settles differently by venue, which turns a
 * locked pair into a loss. The session follows the venues' live feeds ({@link LiveGameWatch}) when the launcher names
 * the match's Polymarket event or Kalshi event, or the rows a session recorded ({@code game_states}) in a replay, or
 * trades at once given neither. A feed quiet for {@code stale_seconds}, or one saying the match isn't live after it
 * was, stops quoting until it says live again. Once a feed says the match is over the bids come down, any unequal
 * position is completed, and the session ends once nothing is in flight and {@code ended_settle_seconds} have passed,
 * or {@code ended_max_seconds} after the end whatever it holds. A match that ends without ever having been live ends
 * the session at once, and one never live {@code never_live_hours} after its scheduled start ends it then. Bids also
 * come down for good once either team's mid is within {@code quote_guard} of a sure thing.
 *
 * <p>Bids rest only while neither price they hang on has moved more than {@code calm_move} in the last
 * {@code calm_seconds} (off unless set). Every price is put on the tick the engine enforces now, rounded toward the
 * book for a take and away from it for a bid.
 *
 * <p>The decision path allocates nothing: books, ladders and plans are arrays reused on every update.
 */
public final class EsportsPmMaker extends StrategyAgent {

    static final int QUOTE = 0;
    static final int COMPLETION = 1;

    static final int OPEN = 0;
    static final int CLOSED = 1;
    static final int ENDING = 2;
    static final int EXIT = 3;

    private static final double PRICE_SCALE = 1e9;
    private static final double SIZE_SCALE = 1e6;
    private static final long SECOND = 1_000_000_000L;
    private static final long EXIT_TIMEOUT_MILLIS = 60_000;
    private static final int LADDER_CAPACITY = 64;
    private static final int MOVE_CAPACITY = 4096;
    private static final int REJECTED_CAPACITY = 64;

    private final String makerVenue;
    private final double quoteSize;
    private final double depthBand;
    private final double requoteSizeChange;
    private final double calmMove;
    private final double quoteGuard;
    private final double minEdge;
    private final double maxLockedUsd;
    private final double completionSlippage;
    private final long completionRetryNanos;
    private final boolean crossCompletions;
    private final boolean bandFromMinSize;
    private final int bookDepth;
    private final long processingTimeNanos;
    private final long giveUpNanos;
    private final long staleNanos;
    private final long endedSettleNanos;
    private final long endedMaxNanos;

    private final ListingFacts facts;
    private final Leg[] legs;
    private final String[] venues;
    private final boolean[] sellRival;
    private final int makerVenueIndex;
    private final Int2ObjectHashMap<Leg> legByListing = new Int2ObjectHashMap<>();
    private GameWatch watch;
    private final String unusable;

    // Per leg: the fee schedule last looked up and its curve rates, so a lookup clones nothing until it changes.
    private final FeeTimeline[] feeTimelines;
    private final FeeSchedule[] feeSchedules;
    private final double[] takerRates;
    private final double[] makerRates;
    private final double[] minSizes;

    // Per maker leg (by team): the hedge ladder, the bid resting, the bid last rejected, and how its prices moved.
    private final RouteLadder[] hedges = {new RouteLadder(LADDER_CAPACITY), new RouteLadder(LADDER_CAPACITY)};
    private final double[] quotedPrice = new double[2];
    private final double[] quotedSize = new double[2];
    private final boolean[] quoted = new boolean[2];
    private final double[] rejectedPrice = new double[2];
    private final double[] rejectedSize = new double[2];
    private final boolean[] rejected = new boolean[2];
    private final MoveWindow[] hedgeMoves = new MoveWindow[2];
    private final MoveWindow[] ownMoves = new MoveWindow[2];

    private final RouteLadder completionLadder = new RouteLadder(LADDER_CAPACITY);
    private final Completion completion = new Completion(8);
    private final MakerQuote makerQuote = new MakerQuote();
    // Completions the venue rejected, as (leg, direction, contracts, limit): never sent again unchanged.
    private final int[] rejectedLeg = new int[REJECTED_CAPACITY];
    private final int[] rejectedDirection = new int[REJECTED_CAPACITY];
    private final long[] rejectedContracts = new long[REJECTED_CAPACITY];
    private final long[] rejectedLimit = new long[REJECTED_CAPACITY];
    private int rejectedCount;

    private final Intent intent = new Intent();
    private final double[] pays = new double[2];

    private long completionWaitUntil;
    private boolean completionWaitsForBook;
    private MatchState game = MatchState.NONE;
    private boolean wasLive;
    private long endedNanos = -1;
    private boolean decided;
    private boolean exiting;

    private MetricStream orders;
    private int orderTs;
    private int orderKind;
    private int orderListing;
    private int orderDirection;
    private int orderContracts;
    private int orderLimit;
    private int orderEdge;
    private int orderProfit;
    private int orderGap;
    private MetricStream gameStates;
    private int gameTs;
    private int gameObserved;
    private int gameStatus;
    private int gameSource;

    /**
     * Arguments are the session config's {@code strategy.args}; any left out takes its default.
     *
     * @param legs one {venue, listing_team_a, listing_team_b} per venue listing the market. Set by the launcher.
     * @param maker_venue the venue quoted (default polymarket).
     * @param quote_size the most contracts bid on one team (200).
     * @param depth_band how far past the anchor hedge level the size may reach (0.01).
     * @param requote_size_change how far the hedge must shrink below a resting bid's size before it is resent smaller.
     * @param calm_seconds the window calm_move is measured over (5).
     * @param calm_move no bid while the hedge cost or the quoted team's best bid has ranged more than this; unset or
     *     0 quotes through every move.
     * @param quote_guard no bid, ever again, once any team's mid is above 1 - quote_guard or below it (0.02); 0 quotes
     *     to the end.
     * @param min_edge the least locked profit per pair, after fees, a bid is priced for (0.01).
     * @param max_locked_usd the most pairs (each paying $1) held before the bids come down (2000).
     * @param completion_slippage how far above the short team's cheapest route a hedge may pay (0.05).
     * @param completion_retry_seconds the least wait after a hedge came back unfilled (0).
     * @param cross_completions whether a hedge is limited at the slippage cap rather than at the level it was costed
     *     on (true).
     * @param band_from_min_size whether the hedge band starts where the hedge first reaches the quoted listing's
     *     smallest order rather than at the cheapest level (true).
     * @param sell_rival_venues venues where a team may also be held by selling the rival's listing (kalshi).
     * @param book_depth book levels read from each update (10).
     * @param processing_time_ns the simulated decision time in a backtest (0).
     * @param league recorded, not used.
     * @param subject_id recorded, not used.
     * @param polymarket_slug the match's Polymarket event, whose feeds say whether it is being played.
     * @param kalshi_event_ticker the match's Kalshi event, whose live data says whether it is being played.
     * @param scheduled_start when the match is scheduled (ISO-8601); with never_live_hours, ends a session whose match
     *     never goes live.
     * @param never_live_hours how long past the scheduled start a match never yet live may be (3).
     * @param stale_seconds no quoting once no feed reporting the match has answered for this long (60).
     * @param ended_settle_seconds once the match is over, how long fills racing the end get (30).
     * @param ended_max_seconds once the match is over, the most the session stays whatever it holds (600).
     * @param game_states rows of [observed_ns, status, source] a session recorded, followed in place of the live feeds.
     */
    @SuppressWarnings({"checkstyle:ParameterName", "checkstyle:ParameterNumber"})
    public EsportsPmMaker(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            List<VenueLegs> legs,
            String maker_venue,
            Double quote_size,
            Double depth_band,
            Double requote_size_change,
            Double calm_seconds,
            Double calm_move,
            Double quote_guard,
            Double min_edge,
            Double max_locked_usd,
            Double completion_slippage,
            Double completion_retry_seconds,
            Boolean cross_completions,
            Boolean band_from_min_size,
            List<String> sell_rival_venues,
            Integer book_depth,
            Long processing_time_ns,
            String league,
            Long subject_id,
            String polymarket_slug,
            String kalshi_event_ticker,
            String scheduled_start,
            Double never_live_hours,
            Double stale_seconds,
            Double ended_settle_seconds,
            Double ended_max_seconds,
            List<List<Object>> game_states) {
        this(
                strategyId,
                marketDataBuffer,
                execReportBuffer,
                intentBuffer,
                positionView,
                securityMaster,
                ListingFacts.of(securityMaster),
                SampledEpochNanoClock.INSTANCE,
                new Settings(
                        legs,
                        maker_venue,
                        quote_size,
                        depth_band,
                        requote_size_change,
                        calm_seconds,
                        calm_move,
                        quote_guard,
                        min_edge,
                        max_locked_usd,
                        completion_slippage,
                        completion_retry_seconds,
                        cross_completions,
                        band_from_min_size,
                        sell_rival_venues,
                        book_depth,
                        processing_time_ns,
                        league,
                        subject_id,
                        polymarket_slug,
                        kalshi_event_ticker,
                        scheduled_start,
                        never_live_hours,
                        stale_seconds,
                        ended_settle_seconds,
                        ended_max_seconds,
                        game_states));
    }

    EsportsPmMaker(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            ListingFacts facts,
            EpochNanoClock clock,
            Settings settings) {
        super(strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster, clock);
        this.facts = facts;
        this.makerVenue = orDefault(settings.makerVenue(), "polymarket");
        this.quoteSize = orDefault(settings.quoteSize(), 200.0);
        this.depthBand = orDefault(settings.depthBand(), 0.01);
        this.requoteSizeChange = orDefault(settings.requoteSizeChange(), 0.5);
        this.calmMove = orDefault(settings.calmMove(), 0.0);
        this.quoteGuard = orDefault(settings.quoteGuard(), 0.02);
        this.minEdge = orDefault(settings.minEdge(), 0.01);
        if (minEdge <= 0) {
            throw new IllegalArgumentException("min_edge must be positive, got " + minEdge);
        }
        this.maxLockedUsd = orDefault(settings.maxLockedUsd(), 2000.0);
        this.completionSlippage = orDefault(settings.completionSlippage(), 0.05);
        this.completionRetryNanos = (long) (orDefault(settings.completionRetrySeconds(), 0.0) * SECOND);
        this.crossCompletions = orDefault(settings.crossCompletions(), Boolean.TRUE);
        this.bandFromMinSize = orDefault(settings.bandFromMinSize(), Boolean.TRUE);
        this.bookDepth = orDefault(settings.bookDepth(), 10);
        this.processingTimeNanos = orDefault(settings.processingTimeNs(), 0L);
        this.giveUpNanos = giveUpNanos(settings.scheduledStart(), orDefault(settings.neverLiveHours(), 3.0));
        this.staleNanos = (long) (orDefault(settings.staleSeconds(), 60.0) * SECOND);
        this.endedSettleNanos = (long) (orDefault(settings.endedSettleSeconds(), 30.0) * SECOND);
        this.endedMaxNanos = (long) (orDefault(settings.endedMaxSeconds(), 600.0) * SECOND);
        double calmSeconds = orDefault(settings.calmSeconds(), 5.0);
        for (int team = 0; team < 2; team++) {
            hedgeMoves[team] = new MoveWindow(calmSeconds, MOVE_CAPACITY);
            ownMoves[team] = new MoveWindow(calmSeconds, MOVE_CAPACITY);
        }

        List<VenueLegs> given = settings.legs() == null ? List.of() : settings.legs();
        List<String> names = new ArrayList<>();
        for (VenueLegs venueLegs : given) {
            if (names.contains(venueLegs.venue())) {
                throw new IllegalArgumentException("legs must name each venue once, got " + given);
            }
            names.add(venueLegs.venue());
        }
        this.venues = names.toArray(new String[0]);
        this.makerVenueIndex = names.indexOf(makerVenue);
        List<String> sellRivalVenues =
                settings.sellRivalVenues() == null ? List.of("kalshi") : settings.sellRivalVenues();
        this.sellRival = new boolean[venues.length];
        this.legs = new Leg[venues.length * 2];
        for (int v = 0; v < venues.length; v++) {
            sellRival[v] = sellRivalVenues.contains(venues[v]);
            this.legs[v * 2] = resolve(v * 2, v, Leg.TEAM_A, given.get(v).listingTeamA());
            this.legs[v * 2 + 1] =
                    resolve(v * 2 + 1, v, Leg.TEAM_B, given.get(v).listingTeamB());
        }
        for (Leg leg : this.legs) {
            legByListing.put(leg.listingId, leg);
        }
        this.feeTimelines = new FeeTimeline[this.legs.length];
        this.feeSchedules = new FeeSchedule[this.legs.length];
        this.takerRates = new double[this.legs.length];
        this.makerRates = new double[this.legs.length];
        this.minSizes = new double[this.legs.length];
        for (Leg leg : this.legs) {
            feeTimelines[leg.index()] = facts.feeTimeline(leg.listingId);
        }
        this.unusable = unusableReason(names);
        this.watch = makeWatch(settings.polymarketSlug(), settings.kalshiEventTicker(), settings.gameStates());
    }

    private static String orDefault(String value, String fallback) {
        return value != null ? value : fallback;
    }

    private static double orDefault(Double value, double fallback) {
        return value != null ? value : fallback;
    }

    private static boolean orDefault(Boolean value, Boolean fallback) {
        return value != null ? value : fallback;
    }

    private static int orDefault(Integer value, int fallback) {
        return value != null ? value : fallback;
    }

    private static long orDefault(Long value, long fallback) {
        return value != null ? value : fallback;
    }

    private static long giveUpNanos(String scheduledStart, double neverLiveHours) {
        if (scheduledStart == null || scheduledStart.isEmpty()) {
            return Long.MAX_VALUE;
        }
        OffsetDateTime start;
        try {
            start = OffsetDateTime.parse(scheduledStart.replace("Z", "+00:00"));
        } catch (DateTimeParseException e) {
            start = LocalDateTime.parse(scheduledStart).atOffset(ZoneOffset.UTC);
        }
        long startNanos = start.toEpochSecond() * SECOND + start.getNano();
        return startNanos + (long) (neverLiveHours * 3600 * SECOND);
    }

    private String unusableReason(List<String> names) {
        if (names.size() < 2) {
            return "only " + names + " list this market; nothing to arb";
        }
        if (makerVenueIndex < 0) {
            return makerVenue + " doesn't list this market; nothing to quote";
        }
        return null;
    }

    private static GameWatch makeWatch(String slug, String ticker, List<List<Object>> gameStates) {
        if (gameStates != null) {
            return new ReplayGameWatch(gameStates);
        }
        boolean hasSlug = slug != null && !slug.isEmpty();
        boolean hasTicker = ticker != null && !ticker.isEmpty();
        if (hasSlug || hasTicker) {
            return LiveGameWatch.start(slug, ticker);
        }
        return null;
    }

    private Leg resolve(int index, int venue, int team, int listingId) {
        Listing listing = facts.listing(listingId);
        if (listing == null) {
            throw new IllegalArgumentException("listing " + listingId + " not found in the registry");
        }
        return new Leg(
                index,
                venue,
                team,
                listingId,
                listing.exchange().exchangeId(),
                listing.security().securityId());
    }

    /** Follows the match on {@code source} instead of what the arguments named. */
    void follow(GameWatch source) {
        if (watch != null) {
            watch.close();
        }
        watch = source;
    }

    @Override
    public long simulateProcessingTime() {
        return processingTimeNanos;
    }

    @Override
    protected void registerMetrics(MetricRecorder metrics) {
        orders = metrics.createStream("esports_arb_orders");
        orderTs = orders.addLongColumn("timestamp");
        orderKind = orders.addLongColumn("kind");
        orderListing = orders.addLongColumn("listing_id");
        orderDirection = orders.addLongColumn("direction");
        orderContracts = orders.addDoubleColumn("contracts");
        orderLimit = orders.addDoubleColumn("limit_price");
        orderEdge = orders.addDoubleColumn("edge_first");
        orderProfit = orders.addDoubleColumn("planned_profit");
        orderGap = orders.addDoubleColumn("gap_before");
        orders.freeze();
        gameStates = metrics.createStream("game_state");
        gameTs = gameStates.addLongColumn("timestamp");
        gameObserved = gameStates.addLongColumn("observed_ns");
        gameStatus = gameStates.addLongColumn("status");
        gameSource = gameStates.addLongColumn("source");
        gameStates.freeze();
    }

    // ---- engine callbacks -------------------------------------------------------------------------------------

    @Override
    protected void onMarketData(StreamRecord data) {
        if (exitIfUnusable()) {
            return;
        }
        int listingId = streams().listingId(data.streamId());
        Leg leg = legByListing.get(listingId);
        if (leg == null || !(data instanceof Mbp10Schema book)) {
            return;
        }
        leg.read(book, bookDepth, data.isBadData());
        if (leg.venue != makerVenueIndex) {
            completionWaitsForBook = false;
        }
        decide(data.getEventTimestamp());
    }

    @Override
    protected void onExecutionReport(OrderExecutionReport report) {
        if (exitIfUnusable()) {
            return;
        }
        Leg leg = legByListing.get(streams().listingId(report.decoder.exchangeId(), (int) report.decoder.securityId()));
        if (leg == null) {
            return;
        }
        OrderStatus status = report.decoder.orderStatus();
        boolean terminal = isTerminal(status);
        if (leg.venue == makerVenueIndex) {
            onQuoteReport(leg.team, status, terminal);
        } else if (terminal) {
            leg.inFlight = false;
            noteOutcome(leg, status, report.decoder.timestampRecv());
        }
        if (terminal || report.decoder.filledQty() > 0) {
            decide(report.decoder.timestampRecv());
        }
    }

    private void onQuoteReport(int team, OrderStatus status, boolean terminal) {
        if (status == OrderStatus.REJECTED) {
            rejected[team] = quoted[team];
            rejectedPrice[team] = quotedPrice[team];
            rejectedSize[team] = quotedSize[team];
        }
        if (terminal) {
            quoted[team] = false;
        }
    }

    private static boolean isTerminal(OrderStatus status) {
        return status == OrderStatus.FILLED
                || status == OrderStatus.CANCELED
                || status == OrderStatus.REJECTED
                || status == OrderStatus.EXPIRED;
    }

    /** A hedge that came back unfilled is planned again only once a hedge book has moved, and never resent if rejected. */
    private void noteOutcome(Leg leg, OrderStatus status, long recvNanos) {
        if (leg.sentKind != COMPLETION || status == OrderStatus.FILLED) {
            return;
        }
        completionWaitUntil = recvNanos + completionRetryNanos;
        completionWaitsForBook = true;
        if (status == OrderStatus.REJECTED) {
            int slot = rejectedCount % REJECTED_CAPACITY;
            rejectedLeg[slot] = leg.index();
            rejectedDirection[slot] = leg.sentDirection;
            rejectedContracts[slot] = leg.sentContracts;
            rejectedLimit[slot] = leg.sentLimit;
            rejectedCount++;
        }
    }

    // ---- decisions --------------------------------------------------------------------------------------------

    void decide(long nowNanos) {
        int gate = gameGate(nowNanos);
        if (gate == EXIT) {
            pullQuotes();
            return;
        }
        for (int team = 0; team < 2; team++) {
            buildHedge(hedges[team], 1 - team, nowNanos);
            sample(team, makerLeg(team), nowNanos);
        }
        if (!anyInFlight()) {
            act(nowNanos, gate);
        }
    }

    /** With nothing in flight: completes an unequal position, else rests, moves or pulls the bids. */
    private void act(long nowNanos, int gate) {
        exposure();
        double gap = pays[0] - pays[1];
        if (Math.abs(gap) >= 1) {
            pullQuotes();
            complete(nowNanos, gap > 0 ? Leg.TEAM_B : Leg.TEAM_A, Math.abs(gap), gap);
        } else if (gate == ENDING) {
            if (!pullQuotes()) {
                leaveOnceSettled(nowNanos);
            }
        } else if (Math.min(pays[0], pays[1]) >= maxLockedUsd || gate != OPEN || allButDecided()) {
            pullQuotes();
        } else {
            requote(Leg.TEAM_A, nowNanos);
            requote(Leg.TEAM_B, nowNanos);
        }
    }

    private void requote(int team, long nowNanos) {
        Leg leg = makerLeg(team);
        if (!target(team, leg, nowNanos)) {
            if (quoted[team]) {
                pull(team, leg);
            }
            return;
        }
        double price = makerQuote.price();
        double size = makerQuote.size();
        if (rejected[team] && rejectedPrice[team] == price && rejectedSize[team] == size) {
            return;
        }
        if (!keepsResting(team, price, size)) {
            quote(team, leg, price, size);
        }
    }

    /**
     * Whether the resting bid already does: a resend loses its place in the queue, so the same price is only resent to
     * shrink it to what the hedge can still cover.
     */
    private boolean keepsResting(int team, double price, double size) {
        return quoted[team]
                && quotedPrice[team] == price
                && (quotedSize[team] == size || size > (1 - requoteSizeChange) * quotedSize[team]);
    }

    /** The bid to rest on the maker venue's team, left in {@link #makerQuote}; false for none. */
    private boolean target(int team, Leg leg, long nowNanos) {
        if (leg.bad || jumpy(team)) {
            return false;
        }
        refreshListing(leg, nowNanos);
        double tick = tick(leg, nowNanos);
        double size = Math.min(quoteSize, maxLockedUsd);
        double minOrder = Math.max(1.0, minSizes[leg.index()]);
        if (!makerQuote.compute(
                hedges[team],
                size,
                minEdge,
                makerRates[leg.index()],
                tick,
                leg.bestAsk(),
                depthBand,
                minOrder,
                bandFromMinSize)) {
            return false;
        }
        if (makerQuote.size() < minOrder) {
            return false;
        }
        double price = compliant(leg, makerQuote.price(), false, nowNanos);
        double bestAsk = leg.bestAsk();
        if (price <= 0 || (!Double.isNaN(bestAsk) && price >= bestAsk)) {
            return false;
        }
        makerQuote.override(price, makerQuote.size());
        return true;
    }

    /** Every way to hold {@code team} on the venues other than the maker's, cheapest first. */
    private void buildHedge(RouteLadder ladder, int team, long nowNanos) {
        ladder.clear();
        for (int v = 0; v < venues.length; v++) {
            if (v != makerVenueIndex) {
                addVenue(ladder, v, team, nowNanos);
            }
        }
        ladder.sort();
    }

    private void addVenue(RouteLadder ladder, int venue, int team, long nowNanos) {
        Leg own = legs[venue * 2 + team];
        Leg rival = legs[venue * 2 + 1 - team];
        if (own.bad || rival.bad) {
            return;
        }
        refreshListing(own, nowNanos);
        ladder.addVenue(own, rival, takerRates[own.index()], sellRival[venue]);
    }

    private void sample(int team, Leg leg, long nowNanos) {
        if (!hedges[team].isEmpty()) {
            hedgeMoves[team].add(nowNanos, hedges[team].allIn(0));
        }
        if (leg.bidCount() > 0 && !leg.bad) {
            ownMoves[team].add(nowNanos, leg.bidPrice(0));
        }
    }

    private boolean jumpy(int team) {
        if (calmMove <= 0) {
            return false;
        }
        return Math.max(hedgeMoves[team].range(), ownMoves[team].range()) > calmMove + 1e-9;
    }

    private boolean allButDecided() {
        if (quoteGuard <= 0 || decided) {
            return decided;
        }
        for (Leg leg : legs) {
            if (leg.bad || leg.bidCount() == 0 || leg.askCount() == 0) {
                continue;
            }
            // The mid, not one side: a stray order on a thin book alone mustn't take the quotes down for good.
            double mid = (leg.bidPrice(0) + leg.askPrice(0)) / 2;
            if (mid > 1 - quoteGuard || mid < quoteGuard) {
                decided = true;
            }
        }
        return decided;
    }

    /** Buys the short team on the hedge venues: none while a retry waits or until a hedge book moves after a miss. */
    private void complete(long nowNanos, int team, double contracts, double gap) {
        if (nowNanos < completionWaitUntil || completionWaitsForBook) {
            return;
        }
        buildHedge(completionLadder, team, nowNanos);
        completion.plan(completionLadder, contracts, completionSlippage, crossCompletions, minSizes);
        for (int i = 0; i < completion.count(); i++) {
            Leg leg = legs[completion.leg(i)];
            boolean buy = completion.direction(i) == RouteLadder.BUY;
            long limit = takeLimit(leg, completion.limit(i), buy, nowNanos);
            long scaledContracts = Math.round(completion.contracts(i) * SIZE_SCALE);
            if (limit < 0 || wasRejected(leg.index(), completion.direction(i), scaledContracts, limit)) {
                continue;
            }
            send(nowNanos, leg, completion.direction(i), scaledContracts, limit, gap);
        }
    }

    private boolean wasRejected(int legIndex, int direction, long contracts, long limit) {
        int stored = Math.min(rejectedCount, REJECTED_CAPACITY);
        for (int i = 0; i < stored; i++) {
            if (rejectedLeg[i] == legIndex
                    && rejectedDirection[i] == direction
                    && rejectedContracts[i] == contracts
                    && rejectedLimit[i] == limit) {
                return true;
            }
        }
        return false;
    }

    /**
     * The order's scaled limit on the tick in force, rounded toward the book (a buy up, a sale down) so it still
     * reaches the level it was costed on, and kept inside (0, 1); -1 where that leaves no price.
     */
    private long takeLimit(Leg leg, double limit, boolean buy, long nowNanos) {
        double tick = tick(leg, nowNanos);
        double bounded = buy ? Math.min(limit, 1 - tick) : Math.max(limit, tick);
        long scaled = Math.round(compliant(leg, bounded, buy, nowNanos) * PRICE_SCALE);
        return scaled > 0 && scaled < (long) PRICE_SCALE ? scaled : -1;
    }

    // ---- match state ------------------------------------------------------------------------------------------

    /** What the match's state allows now (OPEN, CLOSED, ENDING, EXIT), recording each change of it. */
    int gameGate(long nowNanos) {
        if (watch == null) {
            return OPEN;
        }
        MatchState state = watch.state(nowNanos);
        if (!state.equals(game)) {
            game = state;
            recordGame(nowNanos, state);
        }
        wasLive = wasLive || watch.wasLive(nowNanos);
        if (state.status() == MatchState.ENDED) {
            return endedGate(nowNanos, state);
        }
        if (state.status() == MatchState.LIVE) {
            wasLive = true;
        } else if (!wasLive && nowNanos >= giveUpNanos) {
            exit("the match wasn't live by its scheduled start plus never_live_hours");
            return EXIT;
        }
        if (state.status() != MatchState.LIVE || nowNanos - watch.heardNanos(nowNanos) > staleNanos) {
            return CLOSED;
        }
        return OPEN;
    }

    private int endedGate(long nowNanos, MatchState state) {
        if (!wasLive && !holdsAnything()) {
            exit("the match ended without being played (" + MatchState.sourceName(state.source()) + ")");
            return EXIT;
        }
        if (endedNanos < 0) {
            // From when the feed said so, not when a callback first came: books can be quiet after the end.
            endedNanos = state.observedNanos() > 0 ? Math.min(nowNanos, state.observedNanos()) : nowNanos;
        }
        if (nowNanos - endedNanos >= endedMaxNanos) {
            exit("the match is over (" + MatchState.sourceName(state.source()) + "); left after ended_max_seconds");
            return EXIT;
        }
        return ENDING;
    }

    /** Whether the session has an order out or holds a position: a restarted session must still complete it. */
    private boolean holdsAnything() {
        if (anyInFlight()) {
            return true;
        }
        exposure();
        return pays[0] != 0 || pays[1] != 0;
    }

    private void leaveOnceSettled(long nowNanos) {
        if (endedNanos >= 0 && nowNanos - endedNanos >= endedSettleNanos) {
            exit("the match is over (" + MatchState.sourceName(game.source()) + ")");
        }
    }

    private boolean exitIfUnusable() {
        if (exiting) {
            return true;
        }
        if (unusable == null) {
            return false;
        }
        exit(unusable);
        return true;
    }

    private void exit(String reason) {
        if (!exiting) {
            exiting = true;
            if (watch != null) {
                watch.close();
            }
            requestExit(reason, false, EXIT_TIMEOUT_MILLIS, false);
        }
    }

    // ---- state ------------------------------------------------------------------------------------------------

    private Leg makerLeg(int team) {
        return legs[makerVenueIndex * 2 + team];
    }

    private boolean anyInFlight() {
        for (Leg leg : legs) {
            if (leg.inFlight) {
                return true;
            }
        }
        return false;
    }

    /** Contracts paying if team A wins and if team B wins: a long pays when its team wins, a short when it loses. */
    private void exposure() {
        pays[0] = 0;
        pays[1] = 0;
        for (Leg leg : legs) {
            double net = getPositionView().getPosition(leg.listingId).netQuantity / SIZE_SCALE;
            if (net > 0) {
                pays[leg.team] += net;
            } else if (net < 0) {
                pays[1 - leg.team] -= net;
            }
        }
    }

    /** Looks up the leg's fee rates and smallest order as in force now; the clone a new schedule costs happens once. */
    private void refreshListing(Leg leg, long nowNanos) {
        int at = leg.index();
        FeeSchedule schedule = feeTimelines[at].at(nowNanos);
        if (schedule == null) {
            throw new IllegalStateException(
                    "The registry has no fee schedule for listing " + leg.listingId + " at " + nowNanos);
        }
        if (schedule != feeSchedules[at]) {
            FeeComponent[] components = schedule.components();
            if (components.length != 1) {
                throw new IllegalStateException("Listing " + leg.listingId + " fees are not a single price curve");
            }
            feeSchedules[at] = schedule;
            takerRates[at] = components[0].taker();
            makerRates[at] = components[0].maker();
        }
        ListingSpec spec = facts.spec(leg.listingId, nowNanos);
        minSizes[at] = spec != null && spec.minSize() > 0 ? spec.minSize() / SIZE_SCALE : 0.0;
    }

    /** The tick the engine enforces on the listing now; a cent where the registry has none. */
    private double tick(Leg leg, long nowNanos) {
        ListingSpec spec = facts.spec(leg.listingId, nowNanos);
        return spec != null && spec.tickSize() > 0 ? spec.tickSize() / PRICE_SCALE : 0.01;
    }

    /** A price on the tick in force: rounded up when {@code up}, else down. */
    private double compliant(Leg leg, double price, boolean up, long nowNanos) {
        ListingSpec spec = facts.spec(leg.listingId, nowNanos);
        long scaled = Math.round(price * PRICE_SCALE);
        long tick = spec != null ? spec.tickSize() : 0;
        if (tick > 0) {
            scaled = up ? -Math.floorDiv(-scaled, tick) * tick : Math.floorDiv(scaled, tick) * tick;
        }
        return scaled / PRICE_SCALE;
    }

    // ---- orders -----------------------------------------------------------------------------------------------

    private void quote(int team, Leg leg, double price, double size) {
        quoted[team] = true;
        quotedPrice[team] = price;
        quotedSize[team] = size;
        encodeEmpty(leg);
        intent.encoder.bidPrice(Math.round(price * PRICE_SCALE)).bidSize(Math.round(size * SIZE_SCALE));
        intent.encoder.flags().postOnly(true);
        publishIntent(intent);
    }

    private void pull(int team, Leg leg) {
        quoted[team] = false;
        encodeEmpty(leg);
        publishIntent(intent);
    }

    /** Pulls every resting bid; true if any was resting. */
    private boolean pullQuotes() {
        boolean any = false;
        for (int team = 0; team < 2; team++) {
            if (quoted[team]) {
                pull(team, makerLeg(team));
                any = true;
            }
        }
        return any;
    }

    private void send(long nowNanos, Leg leg, int direction, long contracts, long limit, double gap) {
        leg.inFlight = true;
        leg.sentKind = COMPLETION;
        leg.sentDirection = direction;
        leg.sentContracts = contracts;
        leg.sentLimit = limit;
        recordOrder(nowNanos, leg, direction, contracts, limit, gap);
        encodeEmpty(leg);
        intent.encoder
                .takeSide(direction == RouteLadder.BUY ? Side.Bid : Side.Ask)
                .takeSize(contracts)
                .takeOrderType(OrderType.LIMIT)
                .takeLimitPrice(limit);
        publishIntent(intent);
    }

    private void encodeEmpty(Leg leg) {
        intent.encoder
                .exchangeId(leg.exchangeId)
                .securityId(leg.securityId)
                .bidPrice(IntentDecoder.bidPriceNullValue())
                .bidSize(IntentDecoder.bidSizeNullValue())
                .askPrice(IntentDecoder.askPriceNullValue())
                .askSize(IntentDecoder.askSizeNullValue())
                .takeSize(IntentDecoder.takeSizeNullValue())
                .takeSide(Side.NULL_VAL)
                .takeOrderType(OrderType.NULL_VAL)
                .takeLimitPrice(IntentDecoder.takeLimitPriceNullValue());
        intent.encoder.flags().clear();
    }

    private void recordOrder(long nowNanos, Leg leg, int direction, long contracts, long limit, double gap) {
        if (orders == null) {
            return;
        }
        orders.begin();
        orders.setLong(orderTs, nowNanos);
        orders.setLong(orderKind, COMPLETION);
        orders.setLong(orderListing, leg.listingId);
        orders.setLong(orderDirection, direction);
        orders.setDouble(orderContracts, contracts / SIZE_SCALE);
        orders.setDouble(orderLimit, limit / PRICE_SCALE);
        orders.setDouble(orderEdge, Double.NaN);
        orders.setDouble(orderProfit, Double.NaN);
        orders.setDouble(orderGap, gap);
        orders.commit();
    }

    private void recordGame(long nowNanos, MatchState state) {
        if (gameStates == null) {
            return;
        }
        gameStates.begin();
        gameStates.setLong(gameTs, nowNanos);
        gameStates.setLong(gameObserved, state.observedNanos());
        gameStates.setLong(gameStatus, state.status());
        gameStates.setLong(gameSource, state.source());
        gameStates.commit();
    }
}
