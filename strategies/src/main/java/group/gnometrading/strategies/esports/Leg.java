package group.gnometrading.strategies.esports;

import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.book.Mbp10Decoder;

/**
 * One team's listing on one venue: its ids, its book as last seen (prices as probabilities, sizes as contracts), and
 * what the strategy has working there. The book is read into arrays sized once, so an update allocates nothing.
 */
final class Leg {

    static final int TEAM_A = 0;
    static final int TEAM_B = 1;

    private static final double PRICE_SCALE = 1e9;
    private static final double SIZE_SCALE = 1e6;
    private static final int MAX_DEPTH = 10;

    final int venue;
    final int team;
    final int listingId;
    final int exchangeId;
    final int securityId;
    private final int index;

    private final double[] bidPrice = new double[MAX_DEPTH];
    private final double[] bidSize = new double[MAX_DEPTH];
    private final double[] askPrice = new double[MAX_DEPTH];
    private final double[] askSize = new double[MAX_DEPTH];
    private int bidCount;
    private int askCount;

    /** No book yet is no book to trust. */
    boolean bad = true;

    boolean inFlight;
    /** What the order in flight is: {@link EsportsPmMaker#COMPLETION} or a quote; and its key for reject tracking. */
    int sentKind;

    int sentDirection;
    long sentContracts;
    long sentLimit;

    Leg(int index, int venue, int team, int listingId, int exchangeId, int securityId) {
        this.index = index;
        this.venue = venue;
        this.team = team;
        this.listingId = listingId;
        this.exchangeId = exchangeId;
        this.securityId = securityId;
    }

    int index() {
        return index;
    }

    /** Takes the update's book, as deep as {@code depth} and the record go; levels without size are skipped. */
    void read(Mbp10Schema record, int depth, boolean badData) {
        Mbp10Decoder decoder = record.decoder;
        bidCount = 0;
        askCount = 0;
        int levels = Math.min(depth, MAX_DEPTH);
        for (int i = 0; i < levels; i++) {
            long bs = readBidSize(decoder, i);
            if (bs > 0) {
                bidPrice[bidCount] = readBidPrice(decoder, i) / PRICE_SCALE;
                bidSize[bidCount] = bs / SIZE_SCALE;
                bidCount++;
            }
            long as = readAskSize(decoder, i);
            if (as > 0) {
                askPrice[askCount] = readAskPrice(decoder, i) / PRICE_SCALE;
                askSize[askCount] = as / SIZE_SCALE;
                askCount++;
            }
        }
        bad = badData;
    }

    /** Sets the book directly: {bid prices, bid sizes, ask prices, ask sizes}. For tests and replays of recorded tops. */
    void set(double[] bids, double[] bidSizes, double[] asks, double[] askSizes, boolean badData) {
        bidCount = Math.min(bids.length, MAX_DEPTH);
        askCount = Math.min(asks.length, MAX_DEPTH);
        System.arraycopy(bids, 0, bidPrice, 0, bidCount);
        System.arraycopy(bidSizes, 0, bidSize, 0, bidCount);
        System.arraycopy(asks, 0, askPrice, 0, askCount);
        System.arraycopy(askSizes, 0, askSize, 0, askCount);
        bad = badData;
    }

    int bidCount() {
        return bidCount;
    }

    int askCount() {
        return askCount;
    }

    double bidPrice(int level) {
        return bidPrice[level];
    }

    double bidSize(int level) {
        return bidSize[level];
    }

    double askPrice(int level) {
        return askPrice[level];
    }

    double askSize(int level) {
        return askSize[level];
    }

    /** The best ask, or NaN with none. */
    double bestAsk() {
        return askCount > 0 ? askPrice[0] : Double.NaN;
    }

    /** The best bid, or NaN with none. */
    double bestBid() {
        return bidCount > 0 ? bidPrice[0] : Double.NaN;
    }

    private static long readBidPrice(Mbp10Decoder decoder, int level) {
        return switch (level) {
            case 0 -> decoder.bidPrice0();
            case 1 -> decoder.bidPrice1();
            case 2 -> decoder.bidPrice2();
            case 3 -> decoder.bidPrice3();
            case 4 -> decoder.bidPrice4();
            case 5 -> decoder.bidPrice5();
            case 6 -> decoder.bidPrice6();
            case 7 -> decoder.bidPrice7();
            case 8 -> decoder.bidPrice8();
            default -> decoder.bidPrice9();
        };
    }

    private static long readBidSize(Mbp10Decoder decoder, int level) {
        return switch (level) {
            case 0 -> decoder.bidSize0();
            case 1 -> decoder.bidSize1();
            case 2 -> decoder.bidSize2();
            case 3 -> decoder.bidSize3();
            case 4 -> decoder.bidSize4();
            case 5 -> decoder.bidSize5();
            case 6 -> decoder.bidSize6();
            case 7 -> decoder.bidSize7();
            case 8 -> decoder.bidSize8();
            default -> decoder.bidSize9();
        };
    }

    private static long readAskPrice(Mbp10Decoder decoder, int level) {
        return switch (level) {
            case 0 -> decoder.askPrice0();
            case 1 -> decoder.askPrice1();
            case 2 -> decoder.askPrice2();
            case 3 -> decoder.askPrice3();
            case 4 -> decoder.askPrice4();
            case 5 -> decoder.askPrice5();
            case 6 -> decoder.askPrice6();
            case 7 -> decoder.askPrice7();
            case 8 -> decoder.askPrice8();
            default -> decoder.askPrice9();
        };
    }

    private static long readAskSize(Mbp10Decoder decoder, int level) {
        return switch (level) {
            case 0 -> decoder.askSize0();
            case 1 -> decoder.askSize1();
            case 2 -> decoder.askSize2();
            case 3 -> decoder.askSize3();
            case 4 -> decoder.askSize4();
            case 5 -> decoder.askSize5();
            case 6 -> decoder.askSize6();
            case 7 -> decoder.askSize7();
            case 8 -> decoder.askSize8();
            default -> decoder.askSize9();
        };
    }
}
