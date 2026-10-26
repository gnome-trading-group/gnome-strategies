# CLAUDE.md

gnome-strategies is a Java library (no service, no stacks) holding the strategy framework: the `StrategyAgent` base
class, the Python bridge, the factory that builds a strategy from its configured class and args, and a few shared
pieces (the market data collector writers, an MBO book). It contains no concrete production strategies: the live
ones are Python classes in gnomepy-research (`gnomepy_research.sessions.<name>.strategy:<Class>`). A strategy runs as
a session: gnome-launcher asks the registry's `strategy-sessions/launch`, which starts an EC2 instance running
gnome-orchestrator, which builds the strategy through this library inside one JVM with the OMS and gateways.

## Commands

```bash
mvn test                                                             # all tests; spotless + checkstyle run at validate
mvn test -Dtest=group.gnometrading.strategies.StrategyExitTest       # one test class, full class name
mvn spotless:apply                                                   # fix formatting (palantir-java-format)
mvn install                                                          # put the SNAPSHOT in ~/.m2 for gnome-orchestrator/gnome-backtest
```

- Maven, not Gradle. Build config comes from parent `gnome-parent` 1.5.3 (Java 17, spotless, checkstyle, surefire).
- Dependencies resolve from GitHub Packages; `settings.xml` reads `GITHUB_ACTOR` / `GITHUB_TOKEN`.
- Spotless and checkstyle are bound to `validate`, so a formatting slip fails `mvn test`. Run `spotless:apply` first.

## Layout

All code is in `src/main/java/group/gnometrading/`.

Strategy framework (`strategies/`):
- `StrategyAgent` - abstract base. A `GnomeAgent` on a `HOT_PATH` thread. `doWork()` polls exec reports before
  market data so fills are seen before the next tick. Subclasses implement `onMarketData` (book-family records) and
  `onExecutionReport`; optional `onStreamEvent` (non-book streams such as vendor feeds), `onStreamsConfigured`,
  `registerMetrics`. Output is only `publishIntent`, which stamps `timestampSend` and `strategyId`.
- `StrategyControl` - `requestExit(reason, flatten, timeoutMillis)` / `isExiting()`; safe from any thread, first
  request wins.
- `WindDown` - runs after an exit request: cancels quotes on every listing, then (if `flatten`) closes positions
  with MARKET take intents sized to the position, retrying every 1s; gives up on a listing on `INVALID_SIZE`
  (too small) or 5 consecutive rejects.
- `ExitResult` / `ExitListener` / `ExitRequest` - how an exit ended (`held` positions with `KEPT`, `TOO_SMALL`,
  `REJECTED`, `TIMED_OUT`); the orchestrator's `SessionExit` is the listener.
- `StrategyFactory` - builds a Java strategy by class name. Constructor must take the 6 infrastructure params first
  (strategy id, md buffer, exec report buffer, intent buffer, `PositionView`, `SecurityMaster`), then its own args,
  matched to config by parameter name (hence `<parameters>true</parameters>` in `pom.xml`).
- `PythonStrategyAgent` - bridges a JPype proxy (`PythonStrategyCallback`, set via `setCallback` before
  `Orchestrator.main()`) to `StrategyAgent`. Callbacks return `List<Intent>`. Used by gnomepy's strategy runner.

Other packages:
- `collector/` - `MarketDataCollector` (normalized records) and `RawDataCollector` (raw venue frames) write zstd
  minute files into pooled direct memory; `CycleUploader` uploads them to S3 off the data thread, spaced out.
  Used by gnome-orchestrator's `collectors/` (the market data collector process), not by strategies.
- `books/` - `MboBook` (red-black tree of `Limit`s holding `Order`s) and the older `MboBookStrategy` base.
  The first code in the repo; nothing uses it today, since there are no Java strategies yet. Kept for when there are.
- `testing/MarketDataWriter` - dumps records to a local file; used by gnome-orchestrator `MarketDataWriterOrchestrator`.

Tests mirror the packages under `src/test/java`. `StrategyExitTest` covers the wind-down; `StrategyFactoryTest`
the argument matching. `market-data/` holds local sample files and is gitignored.

## Conventions and invariants

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
- Adding a constructor arg to a Java strategy changes its config contract: the factory needs exactly the configured
  arg names, no more, no fewer.

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
  remain), marks the session STOPPING with `stoppedBy: strategy`, closes and exits 0, ending as STOPPED. A WARNING
  lists each held listing and why (`TOO_SMALL`, `REJECTED`, `TIMED_OUT`); those positions need a human decision.
- Release: push to `main` runs `.github/workflows/bump-and-deploy.yml` (gnome-build-tools), which does a
  maven-release-plugin release and publishes to GitHub Packages. Consumers pin versions: gnome-orchestrator
  (sessions and collectors) and gnome-backtest. For gnomepy backtests to see a change, rebuild gnome-backtest (and
  gnome-oms if touched) on matching versions. Live sessions pick it up only via a new orchestrator release.
