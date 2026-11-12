package group.gnometrading.strategies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.SecurityMaster;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StrategyFactoryTest {

    /** A strategy with arguments of several types after the infrastructure. */
    public static final class ConfiguredStrategy extends StrategyAgent {
        final int strategyId;
        final int depth;
        final double threshold;
        final String name;
        final List<Integer> levels;

        public ConfiguredStrategy(
                int strategyId,
                SequencedRingBuffer<?> marketDataBuffer,
                SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
                SequencedRingBuffer<Intent> intentBuffer,
                PositionView positionView,
                SecurityMaster securityMaster,
                int depth,
                double threshold,
                String name,
                List<Integer> levels) {
            super(strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster);
            this.strategyId = strategyId;
            this.depth = depth;
            this.threshold = threshold;
            this.name = name;
            this.levels = levels;
        }

        @Override
        protected void onMarketData(StreamRecord data) {}

        @Override
        protected void onExecutionReport(OrderExecutionReport report) {}
    }

    @Test
    void argumentsAreMatchedByNameAndConverted() {
        StrategyAgent agent = StrategyFactory.createWithOwnBuffers(
                ConfiguredStrategy.class.getName(),
                12,
                null,
                null,
                Map.of("depth", 3L, "threshold", 2, "name", "mm", "levels", List.of(1, 2)));

        ConfiguredStrategy strategy = (ConfiguredStrategy) agent;
        assertEquals(12, strategy.strategyId);
        assertEquals(3, strategy.depth);
        assertEquals(2.0, strategy.threshold);
        assertEquals("mm", strategy.name);
        assertEquals(List.of(1, 2), strategy.levels);
    }

    @Test
    void argumentsThatMatchNoConstructorAreAClearError() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> StrategyFactory.createWithOwnBuffers(
                        ConfiguredStrategy.class.getName(), 1, null, null, Map.of("depth", 3)));
        assertTrue(error.getMessage().contains("[depth]"), error.getMessage());
    }

    /** A strategy whose second argument a launcher sets only for some sessions. */
    public static final class OptionalArgStrategy extends StrategyAgent {
        final String venue;
        final Double threshold;

        public OptionalArgStrategy(
                int strategyId,
                SequencedRingBuffer<?> marketDataBuffer,
                SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
                SequencedRingBuffer<Intent> intentBuffer,
                PositionView positionView,
                SecurityMaster securityMaster,
                String venue,
                Double threshold) {
            super(strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster);
            this.venue = venue;
            this.threshold = threshold;
        }

        @Override
        protected void onMarketData(StreamRecord data) {}

        @Override
        protected void onExecutionReport(OrderExecutionReport report) {}
    }

    @Test
    void aReferenceArgumentLeftOutOfTheConfigIsNull() {
        OptionalArgStrategy strategy = (OptionalArgStrategy) StrategyFactory.createWithOwnBuffers(
                OptionalArgStrategy.class.getName(), 1, null, null, Map.of("venue", "kalshi"));

        assertEquals("kalshi", strategy.venue);
        assertNull(strategy.threshold);
    }

    @Test
    void anArgumentNoParameterTakesIsStillAnError() {
        assertThrows(
                IllegalArgumentException.class,
                () -> StrategyFactory.createWithOwnBuffers(
                        OptionalArgStrategy.class.getName(), 1, null, null, Map.of("venue", "kalshi", "typo", 1)));
    }

    @Test
    void aPrimitiveArgumentMustBeConfigured() {
        assertThrows(
                IllegalArgumentException.class,
                () -> StrategyFactory.createWithOwnBuffers(
                        ConfiguredStrategy.class.getName(),
                        1,
                        null,
                        null,
                        Map.of("threshold", 2, "name", "mm", "levels", List.of())));
    }

    /** A strategy outside the classpath, compiled from source as a session compiles its commit at start. */
    private static final String COMPILED_STRATEGY =
            """
            package compiled;

            import group.gnometrading.SecurityMaster;
            import group.gnometrading.oms.position.PositionView;
            import group.gnometrading.schemas.Intent;
            import group.gnometrading.schemas.OrderExecutionReport;
            import group.gnometrading.schemas.StreamRecord;
            import group.gnometrading.sequencer.SequencedRingBuffer;
            import group.gnometrading.strategies.StrategyAgent;

            public final class Compiled extends StrategyAgent {
                public final String venue;

                public Compiled(
                        int strategyId,
                        SequencedRingBuffer<?> marketDataBuffer,
                        SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
                        SequencedRingBuffer<Intent> intentBuffer,
                        PositionView positionView,
                        SecurityMaster securityMaster,
                        String venue) {
                    super(strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster);
                    this.venue = venue;
                }

                @Override
                protected void onMarketData(StreamRecord data) {}

                @Override
                protected void onExecutionReport(OrderExecutionReport report) {}
            }
            """;

    private static URLClassLoader compiled(Path dir) throws IOException {
        Path source = dir.resolve("src/compiled/Compiled.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, COMPILED_STRATEGY);
        Path classes = dir.resolve("classes");
        Files.createDirectories(classes);
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        int status = javac.run(
                null,
                null,
                null,
                "-parameters",
                "-cp",
                System.getProperty("java.class.path"),
                "-d",
                classes.toString(),
                source.toString());
        assertEquals(0, status, "the test strategy compiles");
        return new URLClassLoader(new URL[] {classes.toUri().toURL()}, StrategyFactory.class.getClassLoader());
    }

    @Test
    void aStrategyCompiledOutsideTheClasspathIsBuiltThroughItsLoader(@TempDir Path dir) throws Exception {
        try (URLClassLoader loader = compiled(dir)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> StrategyFactory.createWithOwnBuffers("compiled.Compiled", 1, null, null, Map.of()),
                    "the host's own loader can't see it");

            StrategyAgent agent = StrategyFactory.create(
                    loader,
                    "compiled.Compiled",
                    3,
                    new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence()),
                    new SequencedRingBuffer<>(OrderExecutionReport::new, new GlobalSequence()),
                    new SequencedRingBuffer<>(Intent::new, new GlobalSequence()),
                    null,
                    null,
                    Map.of("venue", "kalshi"));

            assertEquals(loader, agent.getClass().getClassLoader());
            assertEquals(StrategyAgent.class, agent.getClass().getSuperclass(), "the host's StrategyAgent, not a copy");
            assertEquals("kalshi", agent.getClass().getField("venue").get(agent));
        }
    }

    @Test
    void aClassItsLoaderDoesntHaveIsAClearError(@TempDir Path dir) throws Exception {
        try (URLClassLoader loader = compiled(dir)) {
            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> StrategyFactory.create(
                            loader, "compiled.Missing", 1, null, null, null, null, null, Map.of()));
            assertTrue(error.getMessage().contains("compiled.Missing"), error.getMessage());
        }
    }

    @Test
    void unknownClassIsAClearError() {
        assertThrows(
                IllegalArgumentException.class,
                () -> StrategyFactory.createWithOwnBuffers("no.such.Strategy", 1, null, null, Map.of()));
    }
}
