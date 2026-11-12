package group.gnometrading.strategies.esports;

/**
 * How far a price has ranged over the last {@code seconds}: a book that has just jumped is likely to jump again, and a
 * bid resting through the next jump is filled by whoever saw it first.
 *
 * <p>A ring of (time, price) points. A sample equal to the newest point only moves that point's time forward, so a
 * price that sits still takes one slot however many updates repeat it. When the ring is full the oldest point is
 * dropped, which can only narrow the range.
 */
final class MoveWindow {

    private final long windowNanos;
    private final long[] times;
    private final double[] prices;
    private int head;
    private int size;

    MoveWindow(double seconds, int capacity) {
        this.windowNanos = (long) (seconds * 1e9);
        this.times = new long[capacity];
        this.prices = new double[capacity];
    }

    void add(long nowNanos, double price) {
        if (size > 0) {
            int newest = (head + size - 1) % times.length;
            if (prices[newest] == price) {
                times[newest] = nowNanos;
                expire(nowNanos);
                return;
            }
        }
        if (size == times.length) {
            head = (head + 1) % times.length;
            size--;
        }
        int at = (head + size) % times.length;
        times[at] = nowNanos;
        prices[at] = price;
        size++;
        expire(nowNanos);
    }

    double range() {
        if (size == 0) {
            return 0;
        }
        double low = Double.POSITIVE_INFINITY;
        double high = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < size; i++) {
            double point = prices[(head + i) % times.length];
            low = Math.min(low, point);
            high = Math.max(high, point);
        }
        return high - low;
    }

    private void expire(long nowNanos) {
        while (size > 0 && times[head] < nowNanos - windowNanos) {
            head = (head + 1) % times.length;
            size--;
        }
    }
}
