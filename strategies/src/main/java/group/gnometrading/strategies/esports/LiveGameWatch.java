package group.gnometrading.strategies.esports;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Follows one match live on the venues' own feeds, each on a daemon thread that publishes what it heard through
 * volatile fields, so the strategy thread reads the state without waiting on the network or a lock:
 *
 * <ul>
 *   <li>Polymarket's sports websocket ({@value #SPORTS_WS}, public) pushes every game's {@code live} and
 *       {@code ended} by {@code gameId}; the match's gameId is read from its Polymarket event (Gamma, by slug). A
 *       forfeit never appears on it.
 *   <li>Polymarket's event (Gamma, by slug), polled every {@value #EVENT_POLL_SECONDS} s until it says the match is
 *       over ({@code ended} or {@code closed}): the only Polymarket signal for a match never played. Reported as its
 *       own source, and only once it says ended.
 *   <li>Kalshi's live data for the match's milestone ({@code GET /live_data/{type}/milestone/{id}}, public, polled):
 *       its {@code status} goes unplayed, live, complete. The milestone is found by the event ticker.
 * </ul>
 *
 * <p>A source that fails keeps its last state and retries with backoff, reset once it answers again. Polymarket's
 * text heartbeat ({@code ping}, answered {@code pong}) is answered, and any message, about any game, counts as the
 * socket answering.
 */
final class LiveGameWatch implements GameWatch {

    static final String SPORTS_WS = "wss://sports-api.polymarket.com/ws";
    static final String GAMMA_EVENT = "https://gamma-api.polymarket.com/events?slug=";
    static final String KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2";
    static final int EVENT_POLL_SECONDS = 60;
    static final double KALSHI_POLL_SECONDS = 3.0;
    static final int MAX_BACKOFF_SECONDS = 60;
    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final long PING_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(20);
    private static final long PONG_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final System.Logger LOG = System.getLogger(LiveGameWatch.class.getName());

    /** Reads a URL as JSON; the live one is an HTTP GET, tests pass their own. */
    interface JsonSource {
        JsonNode get(String url) throws Exception;
    }

    private final JsonSource json;
    private final LongSupplier clock;
    private final CountDownLatch stop = new CountDownLatch(1);
    private final List<Thread> threads = new ArrayList<>();
    private final Object lock = new Object();

    // Guarded by lock; indexed by source code.
    private final MatchState[] states = new MatchState[4];
    private final long[] heard = new long[4];

    private volatile MatchState combined = MatchState.NONE;
    private volatile boolean wasLive;
    private volatile long heardNanos;

    LiveGameWatch(JsonSource json, LongSupplier clock) {
        this.json = json;
        this.clock = clock;
    }

    /** Starts following the match on the sources it names; either may be null. */
    static LiveGameWatch start(String polymarketSlug, String kalshiEventTicker) {
        HttpClient http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
        LiveGameWatch watch = new LiveGameWatch(url -> getJson(http, url), LiveGameWatch::epochNanos);
        if (polymarketSlug != null && !polymarketSlug.isEmpty()) {
            watch.spawn("game-state-polymarket", () -> watch.followPolymarket(http, polymarketSlug));
            watch.spawn("game-state-polymarket-event", () -> watch.pollPolymarketEvent(polymarketSlug));
        }
        if (kalshiEventTicker != null && !kalshiEventTicker.isEmpty()) {
            watch.spawn("game-state-kalshi", () -> watch.followKalshi(kalshiEventTicker));
        }
        return watch;
    }

    @Override
    public MatchState state(long nowNanos) {
        return combined;
    }

    @Override
    public boolean wasLive(long nowNanos) {
        return wasLive;
    }

    @Override
    public long heardNanos(long nowNanos) {
        return heardNanos;
    }

    @Override
    public void close() {
        stop.countDown();
    }

    int sources() {
        return threads.size();
    }

    /** Records that a source answered, and what it said; {@code status} below 0 is an answer with no state in it. */
    void set(int source, int status) {
        long now = clock.getAsLong();
        synchronized (lock) {
            heard[source] = now;
            if (status >= 0) {
                MatchState old = states[source];
                if (old == null || old.status() != status) {
                    states[source] = new MatchState(status, source, now);
                    LOG.log(
                            System.Logger.Level.INFO,
                            "game state from {0}: {1}",
                            MatchState.sourceName(source),
                            status);
                }
                if (status == MatchState.LIVE) {
                    wasLive = true;
                }
                combined = MatchState.combine(states);
            }
            long reporting = 0;
            long any = 0;
            for (int s = 0; s < heard.length; s++) {
                any = Math.max(any, heard[s]);
                if (states[s] != null) {
                    reporting = Math.max(reporting, heard[s]);
                }
            }
            heardNanos = reporting > 0 ? reporting : any;
        }
    }

    // ---- Polymarket ---------------------------------------------------------------------------------------------

    /** Handles one sports socket message; returns true when it was the text heartbeat, to be answered with pong. */
    boolean onSportsMessage(String raw, long gameId) {
        if ("ping".equals(raw)) {
            set(MatchState.POLYMARKET, -1);
            return true;
        }
        JsonNode message;
        try {
            message = MAPPER.readTree(raw);
        } catch (Exception e) {
            set(MatchState.POLYMARKET, -1);
            return false;
        }
        if (message.isArray()) {
            for (JsonNode m : message) {
                if (matches(m, gameId)) {
                    set(MatchState.POLYMARKET, polymarketStatus(m));
                    return false;
                }
            }
        } else if (matches(message, gameId)) {
            set(MatchState.POLYMARKET, polymarketStatus(message));
            return false;
        }
        set(MatchState.POLYMARKET, -1);
        return false;
    }

    static int polymarketStatus(JsonNode message) {
        if (message.path("ended").asBoolean(false)) {
            return MatchState.ENDED;
        }
        return message.path("live").asBoolean(false) ? MatchState.LIVE : MatchState.NOT_STARTED;
    }

    static boolean polymarketEventEnded(JsonNode event) {
        return event.path("ended").asBoolean(false) || event.path("closed").asBoolean(false);
    }

    private static boolean matches(JsonNode node, long gameId) {
        JsonNode id = node.get("gameId");
        return node.isObject() && id != null && !id.isNull() && id.asLong() == gameId;
    }

    /** The match's gameId from its Polymarket event, retried until the event has one. */
    long gameId(String slug) throws InterruptedException {
        int attempt = 0;
        while (!stopped()) {
            try {
                JsonNode events = json.get(GAMMA_EVENT + encode(slug));
                JsonNode id =
                        events.isArray() && events.size() > 0 ? events.get(0).get("gameId") : null;
                if (id != null && !id.isNull()) {
                    return id.asLong();
                }
                LOG.log(System.Logger.Level.WARNING, "Polymarket event {0} has no gameId yet", slug);
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "Polymarket event " + slug + " unreadable", e);
            }
            backoff(++attempt);
        }
        return -1;
    }

    private void followPolymarket(HttpClient http, String slug) {
        try {
            long gameId = gameId(slug);
            int attempt = 0;
            while (!stopped()) {
                SportsListener listener = new SportsListener(gameId);
                WebSocket ws = null;
                try {
                    ws = http.newWebSocketBuilder()
                            .connectTimeout(TIMEOUT)
                            .buildAsync(URI.create(SPORTS_WS), listener)
                            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                    attempt = 0;
                    heartbeat(ws, listener);
                } catch (Exception e) {
                    LOG.log(System.Logger.Level.WARNING, "Polymarket sports websocket failed", e);
                } finally {
                    if (ws != null) {
                        ws.abort();
                    }
                }
                if (stopped()) {
                    return;
                }
                LOG.log(System.Logger.Level.WARNING, "Polymarket sports websocket closed; reconnecting");
                backoff(++attempt);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Pings every {@link #PING_INTERVAL_NANOS} until the socket closes, the pongs stop, or the watch stops. */
    private void heartbeat(WebSocket ws, SportsListener listener) throws InterruptedException {
        long lastPong = System.nanoTime();
        while (!stopped() && !listener.closed.isDone()) {
            ws.sendPing(ByteBuffer.allocate(0));
            if (stop.await(PING_INTERVAL_NANOS, TimeUnit.NANOSECONDS)) {
                return;
            }
            long pong = listener.lastPong;
            if (pong != 0) {
                lastPong = pong;
            }
            if (System.nanoTime() - lastPong > PONG_TIMEOUT_NANOS) {
                LOG.log(System.Logger.Level.WARNING, "Polymarket sports websocket stopped answering pings");
                return;
            }
        }
    }

    private void pollPolymarketEvent(String slug) {
        try {
            while (!stopped()) {
                try {
                    JsonNode events = json.get(GAMMA_EVENT + encode(slug));
                    if (events.isArray() && events.size() > 0 && polymarketEventEnded(events.get(0))) {
                        set(MatchState.POLYMARKET_EVENT, MatchState.ENDED);
                        return;
                    }
                } catch (Exception e) {
                    LOG.log(System.Logger.Level.WARNING, "Polymarket event " + slug + " unreadable", e);
                }
                if (stop.await(EVENT_POLL_SECONDS, TimeUnit.SECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private final class SportsListener implements WebSocket.Listener {
        private final long gameId;
        private final StringBuilder partial = new StringBuilder();
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        volatile long lastPong;

        SportsListener(long gameId) {
            this.gameId = gameId;
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                String raw = partial.toString();
                partial.setLength(0);
                if (onSportsMessage(raw, gameId)) {
                    ws.sendText("pong", true);
                }
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket ws, ByteBuffer message) {
            lastPong = System.nanoTime();
            set(MatchState.POLYMARKET, -1);
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            closed.complete(null);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            closed.complete(null);
        }
    }

    // ---- Kalshi -------------------------------------------------------------------------------------------------

    static int kalshiStatus(JsonNode details) {
        String status = details.path("status").asText("").toLowerCase();
        if (status.equals("complete")
                || status.equals("completed")
                || status.equals("closed")
                || status.equals("final")) {
            return MatchState.ENDED;
        }
        if (details.path("is_live").asBoolean(false) || status.equals("live")) {
            return MatchState.LIVE;
        }
        return MatchState.NOT_STARTED;
    }

    /** The live data URL of the event's milestone, retried until Kalshi has one; null once stopped. */
    String kalshiLiveDataUrl(String ticker) throws InterruptedException {
        int attempt = 0;
        while (!stopped()) {
            try {
                JsonNode found = json.get(
                                KALSHI_API + "/milestones?related_event_ticker=" + encode(ticker) + "&limit=5")
                        .path("milestones");
                for (JsonNode m : found) {
                    String id = m.path("id").asText("");
                    if (!id.isEmpty() && contains(m.path("related_event_tickers"), ticker)) {
                        String type = m.path("type").asText("");
                        return KALSHI_API + "/live_data/" + (type.isEmpty() ? "esports_match" : type) + "/milestone/"
                                + id;
                    }
                }
                LOG.log(System.Logger.Level.WARNING, "Kalshi has no milestone for {0} yet", ticker);
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "Kalshi milestone for " + ticker + " unreadable", e);
            }
            backoff(++attempt);
        }
        return null;
    }

    /** Reads Kalshi's live data once; true if it answered. */
    boolean pollKalshi(String url) {
        try {
            set(MatchState.KALSHI, kalshiStatus(json.get(url).path("live_data").path("details")));
            return true;
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "Kalshi live data unreadable: " + url, e);
            return false;
        }
    }

    private void followKalshi(String ticker) {
        try {
            String url = kalshiLiveDataUrl(ticker);
            int attempt = 0;
            while (url != null && !stopped()) {
                if (pollKalshi(url)) {
                    attempt = 0;
                    if (stop.await((long) (KALSHI_POLL_SECONDS * 1000), TimeUnit.MILLISECONDS)) {
                        return;
                    }
                } else {
                    backoff(++attempt);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- plumbing -----------------------------------------------------------------------------------------------

    private void spawn(String name, Runnable body) {
        Thread thread = new Thread(body, name);
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();
    }

    private boolean stopped() {
        return stop.getCount() == 0;
    }

    private void backoff(int attempt) throws InterruptedException {
        stop.await(Math.min(MAX_BACKOFF_SECONDS, 1L << Math.min(attempt, 6)), TimeUnit.SECONDS);
    }

    private static boolean contains(JsonNode array, String value) {
        for (JsonNode v : array) {
            if (value.equals(v.asText())) {
                return true;
            }
        }
        return false;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static long epochNanos() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }

    private static JsonNode getJson(HttpClient http, String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(TIMEOUT)
                .header("User-Agent", "gnome-session")
                .GET()
                .build();
        HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() >= 400) {
            throw new IllegalStateException("HTTP " + response.statusCode() + " from " + url);
        }
        return MAPPER.readTree(response.body());
    }
}
