package group.gnometrading.strategies.esports;

import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.md.Stream;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.oms.position.Position;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.book.Mbp10Encoder;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.sm.Security;
import group.gnometrading.sm.fee.FeeComponent;
import group.gnometrading.sm.fee.FeeKind;
import group.gnometrading.sm.fee.FeeRounding;
import group.gnometrading.sm.fee.FeeSchedule;
import group.gnometrading.sm.fee.FeeSide;
import group.gnometrading.sm.fee.FeeTimeline;
import group.gnometrading.strategies.ExitResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.collections.Int2ObjectHashMap;

/**
 * Runs an {@link EsportsPmMaker} through its buffers as the engine does, on the Python tests' fixtures: three venues
 * listing one market, team A first on each. Polymarket International (exchange 4) is quoted; Kalshi (5) and Polymarket
 * US (6) hedge. Listing 101 is Polymarket International's team A, its security 1101, its stream 10101, and so on.
 */
final class MakerHarness {

    static final long SECOND = 1_000_000_000L;
    static final double PRICE = 1e9;
    static final double SIZE = 1e6;
    static final int PMI = 4;
    static final int KALSHI = 5;
    static final int PMUS = 6;
    static final int[] LISTINGS = {101, 102, 201, 202, 301, 302};
    static final List<Map<String, Object>> LEGS = List.of(
            Map.of("venue", "polymarket", "listing_team_a", 101, "listing_team_b", 102),
            Map.of("venue", "kalshi", "listing_team_a", 201, "listing_team_b", 202),
            Map.of("venue", "polymarket_us", "listing_team_a", 301, "listing_team_b", 302));

    /** The maker tests' books: (listing, asks, bids), each a list of {price, size}. */
    static final Object[][] BOOKS = {
        {101, new double[][] {{0.60, 100}}, new double[][] {{0.40, 100}}},
        {102, new double[][] {{0.62, 100}}, new double[][] {{0.38, 100}}},
        {201, new double[][] {{0.55, 100}}, new double[][] {{0.40, 100}}},
        {202, new double[][] {{0.50, 100}}, new double[][] {{0.42, 100}}},
        {301, new double[][] {{0.56, 100}}, new double[][] {{0.50, 100}}},
        {302, new double[][] {{0.48, 60}}, new double[][] {{0.44, 100}}},
    };

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** What one intent asked for, as published. */
    record Sent(
            int exchangeId,
            long securityId,
            long bidPrice,
            long bidSize,
            boolean postOnly,
            Side takeSide,
            long takeSize,
            OrderType takeOrderType,
            long takeLimit) {

        boolean isBid() {
            return bidSize != IntentDecoder.bidSizeNullValue() && bidSize > 0;
        }

        boolean isTake() {
            return takeSize != IntentDecoder.takeSizeNullValue() && takeSize > 0;
        }

        boolean isPull() {
            return !isBid() && !isTake();
        }

        double bid() {
            return bidPrice / PRICE;
        }

        double bidContracts() {
            return bidSize / SIZE;
        }

        double limit() {
            return takeLimit / PRICE;
        }

        double takeContracts() {
            return takeSize / SIZE;
        }
    }

    final Int2ObjectHashMap<Position> positions = new Int2ObjectHashMap<>();
    final List<String> exits = new ArrayList<>();
    final EsportsPmMaker strategy;
    /** Off, intents are drained without being kept, so a measurement sees only what the strategy allocates. */
    boolean recording = true;
    /** What the strategy's clock reads: the engine stamps each intent with it. */
    long clock;

    private final List<Sent> sent = new ArrayList<>();
    private final SequencedPoller intents;
    private final Mbp10Schema book = new Mbp10Schema();
    private final OrderExecutionReport report = new OrderExecutionReport();

    MakerHarness(Map<String, Object> args) {
        this(args, minSizes(5.0));
    }

    MakerHarness(Map<String, Object> args, Map<Integer, Double> minSizes) {
        Map<String, Object> all = new HashMap<>(args);
        all.putIfAbsent("legs", LEGS);
        Settings settings = MAPPER.convertValue(all, Settings.class);
        for (int listingId : LISTINGS) {
            Position position = new Position();
            position.init(listingId);
            positions.put(listingId, position);
        }
        GlobalSequence sequence = new GlobalSequence();
        SequencedRingBuffer<Intent> intentBuffer = new SequencedRingBuffer<>(Intent::new, sequence);
        Intent decoded = new Intent();
        intents = intentBuffer.createPoller((seq, templateId, buffer, length) -> {
            if (!recording) {
                return;
            }
            decoded.wrap(buffer);
            IntentDecoder d = decoded.decoder;
            sent.add(new Sent(
                    d.exchangeId(),
                    d.securityId(),
                    d.bidPrice(),
                    d.bidSize(),
                    d.flags().postOnly(),
                    d.takeSide(),
                    d.takeSize(),
                    d.takeOrderType(),
                    d.takeLimitPrice()));
        });
        FakeFacts facts = new FakeFacts(minSizes);
        strategy = new EsportsPmMaker(
                7,
                new SequencedRingBuffer<>(Mbp10Schema::new, sequence),
                new SequencedRingBuffer<>(OrderExecutionReport::new, sequence),
                intentBuffer,
                positions::get,
                null,
                facts,
                () -> clock,
                settings);
        StreamDirectory directory = new StreamDirectory();
        List<Listing> listings = new ArrayList<>();
        for (int listingId : LISTINGS) {
            Listing listing = facts.listing(listingId);
            listings.add(listing);
            directory.add(stream(listingId), listing);
        }
        strategy.configureStreams(directory);
        strategy.configureExit(listings, (ExitResult result) -> exits.add(result.reason()));
    }

    static Map<Integer, Double> minSizes(double polymarketIntl) {
        return Map.of(101, polymarketIntl, 102, polymarketIntl);
    }

    static int exchangeOf(int listingId) {
        return listingId < 200 ? PMI : listingId < 300 ? KALSHI : PMUS;
    }

    static long securityOf(int listingId) {
        return 1000 + listingId;
    }

    /** Feeds books given as {listing, asks, bids} at {@code t}; returns what the strategy sent in reply. */
    List<Sent> feed(Object[][] books, long t) {
        List<Sent> out = new ArrayList<>();
        for (Object[] b : books) {
            out.addAll(book((Integer) b[0], (double[][]) b[1], (double[][]) b[2], t, false));
        }
        return out;
    }

    List<Sent> book(int listingId, double[][] asks, double[][] bids, long t, boolean bad) {
        submitBook(listingId, asks, bids, t, bad);
        return pass();
    }

    List<Sent> report(int listingId, OrderStatus status, double filled, long t) {
        submitReport(listingId, status, filled, t);
        return pass();
    }

    /** Runs the agent once and drains its intents, keeping nothing. */
    void step() {
        try {
            strategy.doWork();
            intents.poll();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    void submitBook(int listingId, double[][] asks, double[][] bids, long t, boolean bad) {
        Mbp10Encoder e = book.encoder;
        e.streamId(10_000 + listingId);
        e.timestampEvent(t);
        e.timestampRecv(t);
        e.flags().clear();
        e.flags().badData(bad);
        for (int level = 0; level < 10; level++) {
            setLevel(e, level, asks, bids);
        }
        strategy.submitMarketData(book);
    }

    void submitReport(int listingId, OrderStatus status, double filled, long t) {
        report.encoder
                .exchangeId(exchangeOf(listingId))
                .securityId(securityOf(listingId))
                .orderStatus(status)
                .execType(execType(status, filled))
                .filledQty(Math.round(filled * SIZE))
                .timestampRecv(t)
                .timestampEvent(t);
        strategy.submitExecReport(report);
    }

    void hold(int listingId, double contracts) {
        positions.get(listingId).netQuantity = Math.round(contracts * SIZE);
    }

    /** Runs the agent twice, so an exit the first pass asked for has its wind-down run (and its listener told). */
    List<Sent> pass() {
        sent.clear();
        try {
            strategy.doWork();
            intents.poll();
            strategy.doWork();
            intents.poll();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return new ArrayList<>(sent);
    }

    /** The bids in {@code sent}, by security: {price, contracts}; a pull is {0, 0}. */
    static Map<Long, double[]> quotes(List<Sent> sent) {
        Map<Long, double[]> out = new HashMap<>();
        for (Sent s : sent) {
            if (s.exchangeId() == PMI) {
                out.put(s.securityId(), s.isBid() ? new double[] {s.bid(), s.bidContracts()} : new double[] {0, 0});
            }
        }
        return out;
    }

    static List<Sent> takes(List<Sent> sent) {
        return sent.stream().filter(Sent::isTake).toList();
    }

    private static ExecType execType(OrderStatus status, double filled) {
        if (status == OrderStatus.REJECTED) {
            return ExecType.REJECT;
        }
        if (status == OrderStatus.CANCELED) {
            return ExecType.CANCEL;
        }
        if (status == OrderStatus.FILLED) {
            return ExecType.FILL;
        }
        return filled > 0 ? ExecType.PARTIAL_FILL : ExecType.NEW;
    }

    private static void setLevel(Mbp10Encoder e, int level, double[][] asks, double[][] bids) {
        long askPrice = level < asks.length ? Math.round(asks[level][0] * PRICE) : 0;
        long askSize = level < asks.length ? Math.round(asks[level][1] * SIZE) : 0;
        long bidPrice = level < bids.length ? Math.round(bids[level][0] * PRICE) : 0;
        long bidSize = level < bids.length ? Math.round(bids[level][1] * SIZE) : 0;
        switch (level) {
            case 0 -> e.askPrice0(askPrice)
                    .askSize0(askSize)
                    .bidPrice0(bidPrice)
                    .bidSize0(bidSize);
            case 1 -> e.askPrice1(askPrice)
                    .askSize1(askSize)
                    .bidPrice1(bidPrice)
                    .bidSize1(bidSize);
            case 2 -> e.askPrice2(askPrice)
                    .askSize2(askSize)
                    .bidPrice2(bidPrice)
                    .bidSize2(bidSize);
            case 3 -> e.askPrice3(askPrice)
                    .askSize3(askSize)
                    .bidPrice3(bidPrice)
                    .bidSize3(bidSize);
            case 4 -> e.askPrice4(askPrice)
                    .askSize4(askSize)
                    .bidPrice4(bidPrice)
                    .bidSize4(bidSize);
            case 5 -> e.askPrice5(askPrice)
                    .askSize5(askSize)
                    .bidPrice5(bidPrice)
                    .bidSize5(bidSize);
            case 6 -> e.askPrice6(askPrice)
                    .askSize6(askSize)
                    .bidPrice6(bidPrice)
                    .bidSize6(bidSize);
            case 7 -> e.askPrice7(askPrice)
                    .askSize7(askSize)
                    .bidPrice7(bidPrice)
                    .bidSize7(bidSize);
            case 8 -> e.askPrice8(askPrice)
                    .askSize8(askSize)
                    .bidPrice8(bidPrice)
                    .bidSize8(bidSize);
            default -> e.askPrice9(askPrice)
                    .askSize9(askSize)
                    .bidPrice9(bidPrice)
                    .bidSize9(bidSize);
        }
    }

    private static Stream stream(int listingId) {
        return new Stream(10_000 + listingId, 1, "SRC", "LISTING", listingId, null, "BOOK", null, 0, 0, 0, true);
    }

    /** The registry's facts on the fixture listings: a one-cent tick, the venues' fee curves, and minimum orders. */
    static final class FakeFacts implements ListingFacts {
        private final Int2ObjectHashMap<Listing> listings = new Int2ObjectHashMap<>();
        private final Int2ObjectHashMap<FeeTimeline> fees = new Int2ObjectHashMap<>();
        private final Int2ObjectHashMap<ListingSpec> specs = new Int2ObjectHashMap<>();

        FakeFacts(Map<Integer, Double> minSizes) {
            for (int listingId : LISTINGS) {
                int exchangeId = exchangeOf(listingId);
                listings.put(
                        listingId,
                        new Listing(
                                listingId,
                                new Exchange(exchangeId, "EX" + exchangeId, "Exchange", "us-east-1"),
                                new Security(
                                        (int) securityOf(listingId),
                                        "SEC",
                                        null,
                                        null,
                                        null,
                                        "USD",
                                        "USD",
                                        "USD",
                                        false,
                                        false,
                                        0,
                                        0,
                                        true,
                                        0),
                                String.valueOf(listingId),
                                "SEC"));
                double taker = exchangeId == PMI ? 0.05 : exchangeId == KALSHI ? 0.07 : 0.0695;
                fees.put(listingId, timeline(taker, 0.0));
                long minSize = Math.round(minSizes.getOrDefault(listingId, 0.0) * SIZE);
                specs.put(listingId, new ListingSpec(listingId, 10_000_000L, 0, 0, 1, minSize));
            }
        }

        static FeeTimeline timeline(double taker, double maker) {
            FeeSchedule schedule = new FeeSchedule(
                    "USD",
                    new FeeComponent[] {new FeeComponent(FeeKind.PRICE_CURVE, taker, maker, 1.0, FeeSide.BOTH)},
                    FeeRounding.NONE);
            return new FeeTimeline(new long[] {0}, new long[] {FeeTimeline.OPEN_ENDED}, new FeeSchedule[] {schedule});
        }

        @Override
        public Listing listing(int listingId) {
            return listings.get(listingId);
        }

        @Override
        public FeeTimeline feeTimeline(int listingId) {
            return fees.get(listingId);
        }

        @Override
        public ListingSpec spec(int listingId, long epochNanos) {
            return specs.get(listingId);
        }
    }
}
