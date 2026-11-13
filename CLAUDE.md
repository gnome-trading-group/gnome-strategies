# CLAUDE.md

gnome-strategies holds two things with two lifecycles:

- `runtime/` - the strategy runtime: the `StrategyAgent` base class, the Python bridge, the factory that builds a
  strategy from its configured class and args, and the exit wind-down. A Java
  library (no service, no stacks), released as the `gnome-strategies` artifact and pinned by gnome-orchestrator
  (sessions) and gnome-backtest.
- `strategies/` - the production Java strategies (`EsportsPmMaker` ...). Never released. A session or backtest runs
  them by commit, built against the orchestrator release `strategies/pom.xml` pins (`platform.orchestrator`), which
  the registry launcher reads from the pom at the commit; a java session runs that jar alone, JVM options included.
  A separate Maven project, deliberately not part of the root build.

Most live strategies are still Python classes in gnomepy-research (`gnomepy_research.sessions.<name>.strategy:<Class>`).
A strategy runs as a session: gnome-launcher asks the registry's `strategy-sessions/launch`, which starts an EC2
instance running gnome-orchestrator, which builds the strategy through the runtime inside one JVM with the OMS and
gateways.

## Commands

```bash
mvn test                                                             # runtime: all tests; spotless + checkstyle at validate
mvn test -Dtest=group.gnometrading.strategies.StrategyExitTest       # one runtime test class, full class name
mvn spotless:apply                                                   # fix formatting (palantir-java-format)
mvn install                                                          # runtime SNAPSHOT into ~/.m2 for gnome-orchestrator/gnome-backtest

mvn -f strategies/pom.xml test                                       # strategies, against the pinned platform
mvn -f strategies/pom.xml spotless:apply
mvn -f strategies/pom.xml test -Dplatform.orchestrator=<version>     # against another orchestrator, e.g. a local build
```

- Maven, not Gradle. Build config comes from parent `gnome-parent` 1.7.0 (Java 17, spotless, checkstyle, surefire).
- Dependencies resolve from GitHub Packages; `settings.xml` reads `GITHUB_ACTOR` / `GITHUB_TOKEN` (CI). Locally,
  plain `mvn` uses `~/.m2/settings.xml`; the GitHub CLI's token has no `read:packages` scope.
- Spotless and checkstyle are bound to `validate`, so a formatting slip fails `mvn test`. Run `spotless:apply` first.

## Layout

`runtime/src/main/java/group/gnometrading/`:

Strategy runtime (`strategies/`):
- `StrategyAgent` - abstract base. A `GnomeAgent` on a `HOT_PATH` thread. `doWork()` polls exec reports before
  market data so fills are seen before the next tick. Subclasses implement `onMarketData` (book-family records) and
  `onExecutionReport`; optional `onStreamEvent` (non-book streams such as vendor feeds), `onStreamsConfigured`,
  `registerMetrics`. Output is only `publishIntent`, which stamps `timestampSend` and `strategyId`, and the trigger:
  the event being handled when the intent was published (its global sequence, `TriggerKind`, ring arrival from the
  poller, platform receipt from the record's or report's `timestampRecv`) and when handling began. An intent published
  outside an event (a wind-down step) is `OTHER` with no trigger. The OMS copies the trigger onto every order it sends.
  Given latency grids (`measureLatency`, by the orchestrator) it records `strat.transit` (platform receipt to ring
  arrival, across hosts), `strat.queue` and `strat.decision` per exchange and kind; `activity()` counts market-data,
  stream and execution-report events and intents sent (gnome-core `MetricCounters`, read by the session's samples).
- `StrategyControl` - `requestExit(reason, flatten, timeoutMillis, notifyExit)` / `isExiting()`; safe from any thread,
  first request wins.
- `WindDown` - runs after an exit request: cancels quotes on every listing, then (if `flatten`) closes positions
  with MARKET take intents sized to the position, retrying every 1s; gives up on a listing on `INVALID_SIZE`
  (too small) or 5 consecutive rejects.
- `ExitResult` / `ExitListener` / `ExitRequest` - how an exit ended (`held` positions with `KEPT`, `TOO_SMALL`,
  `REJECTED`, `TIMED_OUT`); the orchestrator's `SessionExit` is the listener.
- `StrategyFactory` - builds a Java strategy by class name. Constructor must take the 6 infrastructure params first
  (strategy id, md buffer, exec report buffer, intent buffer, `PositionView`, `SecurityMaster`), then its own args,
  matched to config by parameter name (hence `<parameters>true</parameters>` in both poms). Every configured arg must
  name a parameter; a reference-type parameter left out of the config is passed null (launchers set some args only for
  some sessions), a primitive one must be configured. Generic parameters (`List<SomeRecord>`) are converted through
  Jackson element by element.
- `PythonStrategyAgent` - bridges a JPype proxy (`PythonStrategyCallback`, set via `setCallback` before
  `Orchestrator.main()`) to `StrategyAgent`. Callbacks return `List<Intent>`. Used by gnomepy's strategy runner.

`strategies/src/main/java/group/gnometrading/strategies/`:

- `esports/`:
  - `EsportsPmMaker` - esports cross-venue arb, maker on Polymarket International (port of gnomepy-research's
    `esports_cross_venue_arb__pm_maker`): rests a post-only bid on each team priced off the cheapest hedge on the other
    venues (`MakerQuote`, band anchored where the hedge reaches the quoted listing's minimum order), and on a fill buys
    the other team at once (`Completion`, limited at the cheapest route + `completion_slippage` so it crosses; an
    unfilled hedge is re-planned on the next hedge-venue book update). Play gate from the venues' live feeds
    (`LiveGameWatch`: Polymarket sports websocket, Gamma event poll, Kalshi milestone live data, on daemon threads
    publishing through volatiles) or recorded `game_states` (`ReplayGameWatch`). Config args are snake_case to match the
    launcher's (`@SuppressWarnings("checkstyle:ParameterName")` on the public constructor), packed into `Settings`;
    registry reads go through `ListingFacts` so tests (and the allocation test) use an in-memory fake.
  - Metric streams `esports_arb_orders` and `game_state` keep the Python strategy's names and columns, so either can
    replay the other's sessions.

Tests mirror the packages under each module's `src/test/java`. `StrategyExitTest` covers the wind-down;
`StrategyFactoryTest` the argument matching. In `strategies/`, `MakerHarness` drives the maker through its buffers on
the Python tests' fixtures, and `EsportsPmMakerFactoryTest` builds it from a launcher-shaped config through the
platform's own `StrategyFactory`, so a pin to a release that can't run the maker fails the build. `market-data/` holds
local sample files and is gitignored.

## Conventions and invariants

- Platform changes go one way: change `runtime/`, release it, pin it in gnome-orchestrator and gnome-backtest,
  release those, then bump `platform.orchestrator` in `strategies/pom.xml`. `strategies/` never builds against `runtime/`'s
  source, so a strategy needing a new runtime feature fails to compile until that path is done: at build time, not
  in a session.
- Production code only. Backtest drivers, simulation and fill models live in gnome-backtest/gnomepy. The existing
  hooks the backtest driver needs (`simulateProcessingTime`, `submitMarketData`, `submitExecReport`,
  `getIntentBuffer`, `getMarketDataBuffer`, `createWithOwnBuffers`, `PythonStrategyAgent.create`) are the boundary;
  don't grow it. The same agent must behave identically live and in a backtest.
- Hot path is zero-allocation: `StrategyAgent` reads through pre-allocated flyweights (one `StreamRecord` per
  template id, one `OrderExecutionReport`), and `WindDown` allocates nothing until it finishes. Keep new per-event
  code allocation-free (no boxing, lambdas capturing state, string building, or collections). The Python path
  allocates by nature.
- Strategies emit `Intent`s, not orders. An intent is the desired quote (bid/ask price and size) plus an optional
  take for a listing; the OMS (`gnome-oms` `IntentResolver`, one bid and one ask slot per listing) diffs it against
  live orders and sends new/modify/cancel. Null-valued fields mean "no quote on that side".
- Scaled integers: prices and money are x1e9, sizes and quantities are x1e6.
- Java 17 (from the parent pom).
- `doWork()` and `onSequencedEvent()` are final: put logic in the callbacks. After an exit is requested the
  callbacks stop, but buffers are still drained so upstream writers never block.
- Config: session config keys become env vars (`.` to `_`, uppercased) in the registry launcher. Structured values go
  as one JSON env var: every `strategy.args.*` key is collected into `STRATEGY_ARGS_JSON` (the orchestrator falls back
  to `strategy.args.*` properties when it's unset), and `simulation` becomes `SIMULATION_JSON`.
- Adding a constructor arg to a Java strategy changes its config contract: a configured name with no parameter fails
  the session, so remove args from rules before removing parameters. Make an arg a boxed/reference type with a default
  applied in the strategy unless every config must set it.
- Time is gnome-core's `SampledEpochNanoClock` (allocation-free); gnome-parent's checkstyle bans Agrona's
  `SystemEpochNanoClock`, which builds an `Instant` per read. Allocation tests still pass their own clock through the
  7-arg constructor, so they measure only the strategy's code.

## Operations

- No stacks. This repo deploys nothing; it is consumed as a Maven artifact.
- Identity: a strategy is a row in the registry's `strategy.strategy` (`strategy_id`, unique `name`). Each session
  (`strategy.session`) carries the id plus a config with `strategy.type` (`java` or `python`) and `strategy.class`
  (Java FQCN, or Python `module.path:ClassName`). The launcher rejects a session missing either. The orchestrator
  reads them as `STRATEGY_ID`, `STRATEGY_TYPE`, `STRATEGY_CLASS`.
- Logs: session logs go to CloudWatch `/gnome/orchestrator/<region>` (the instance's region, near the venue).
- Startup failures here look like `Strategy class not found: ...`, `No constructor of ... takes the strategy
  infrastructure followed by exactly [...]` (args don't match the constructor), `Failed to parse STRATEGY_ARGS_JSON`,
  or `Python strategy callback not set`. Check the session config's `strategy.class` and `strategy.args.*`.
- Runtime failure: any exception from a strategy callback reaches the session error handler, which logs
  `Agent error: ...` (FATAL_ERROR_EXITING) and exits 1; the session is marked FAILED. Look for the stack trace just
  before that line.
- Risk halts (kill switch, latched limits) happen in the OMS, not here. A halted strategy keeps running and keeps
  getting callbacks; its orders come back as synthetic REJECTs (`RISK_LIMIT_EXCEEDED`) and the log shows
  `ORDER_REJECTED_HALTED` with a cause code. It stays halted until an operator resumes it. See gnome-oms's CLAUDE.md.
- Graceful exit: a strategy calling `requestExit` stops its callbacks, cancels quotes, optionally flattens until the
  timeout, then `SessionExit` raises a `SESSION_EXITED` alert (INFO, or WARNING if asked to flatten and positions
  remain; a clean INFO exit is labelled `route: silent`, recorded on the Alerts page but not posted to Slack, unless
  `notifyExit`), marks the session STOPPING with `stoppedBy: strategy`, closes and exits 0, ending as STOPPED. A WARNING
  lists each held listing and why (`TOO_SMALL`, `REJECTED`, `TIMED_OUT`); those positions need a human decision.
- Release: a push to `main` touching `runtime/`, the root `pom.xml` or `settings.xml` runs
  `.github/workflows/bump-and-deploy.yml` (gnome-build-tools): a maven-release-plugin release of the parent and
  `runtime/`, published to GitHub Packages. Consumers pin versions: gnome-orchestrator (sessions) and
  gnome-backtest. A push touching only `strategies/` releases nothing.
- CI: `strategies.yml` tests `strategies/` on every push to any branch against its pinned platform;
  `runtime-tests.yml` tests `runtime/` on branches other than main.
- `strategy/*` branches may be pushed to run a backtest or a paper session from a commit.
