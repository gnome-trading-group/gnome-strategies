package group.gnometrading.strategies.esports;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * The states a session recorded, as of each moment of a replay: rows of {@code [observed_ns, status, source]}, the
 * Python strategy's {@code game_states} argument. A replay's feed is always fresh, since the rows are all it has.
 */
final class ReplayGameWatch implements GameWatch {

    private final long[] times;
    private final MatchState[] states;
    private final long firstLiveNanos;

    ReplayGameWatch(List<List<Object>> rows) {
        MatchState[] parsed = new MatchState[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            List<Object> row = rows.get(i);
            parsed[i] = new MatchState(
                    MatchState.statusCode(String.valueOf(row.get(1))),
                    MatchState.sourceCode(String.valueOf(row.get(2))),
                    ((Number) row.get(0)).longValue());
        }
        Arrays.sort(parsed, Comparator.comparingLong(MatchState::observedNanos));
        this.states = parsed;
        this.times = new long[parsed.length];
        long live = Long.MAX_VALUE;
        for (int i = 0; i < parsed.length; i++) {
            times[i] = parsed[i].observedNanos();
            if (parsed[i].status() == MatchState.LIVE) {
                live = Math.min(live, parsed[i].observedNanos());
            }
        }
        this.firstLiveNanos = live;
    }

    @Override
    public MatchState state(long nowNanos) {
        int at = lastAtOrBefore(nowNanos);
        return at < 0 ? MatchState.NONE : states[at];
    }

    @Override
    public boolean wasLive(long nowNanos) {
        return firstLiveNanos <= nowNanos;
    }

    @Override
    public long heardNanos(long nowNanos) {
        return nowNanos;
    }

    @Override
    public void close() {}

    private int lastAtOrBefore(long nowNanos) {
        int lo = 0;
        int hi = times.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (times[mid] <= nowNanos) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo - 1;
    }
}
