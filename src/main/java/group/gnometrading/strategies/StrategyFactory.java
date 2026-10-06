package group.gnometrading.strategies;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.SecurityMaster;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Builds a Java strategy from its class name and configured arguments, the same way live and in a backtest.
 *
 * <p>A strategy's constructor takes the infrastructure first, in this order: strategy id, market data buffer, exec
 * report buffer, intent buffer, position view, security master. Its own arguments follow and are matched to the
 * configured ones by parameter name, so the class must be compiled with {@code -parameters}. Values are converted
 * to each parameter's type: numbers are narrowed or widened, and anything else is converted as JSON would be.
 */
public final class StrategyFactory {

    private static final int INFRASTRUCTURE_PARAMS = 6;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StrategyFactory() {}

    public static StrategyAgent create(
            String className,
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            Map<String, Object> strategyArgs) {
        final Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Strategy class not found: " + className, e);
        }
        for (Constructor<?> ctor : clazz.getConstructors()) {
            Object[] args = matchArguments(
                    ctor,
                    strategyId,
                    marketDataBuffer,
                    execReportBuffer,
                    intentBuffer,
                    positionView,
                    securityMaster,
                    strategyArgs);
            if (args != null) {
                try {
                    return (StrategyAgent) ctor.newInstance(args);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("Failed to construct strategy " + className, e);
                }
            }
        }
        throw new IllegalArgumentException("No constructor of " + className + " takes the strategy infrastructure "
                + "followed by exactly " + strategyArgs.keySet() + ". Ensure the class is compiled with -parameters.");
    }

    /** As {@link #create}, with buffers of the strategy's own; for a backtest, which feeds them directly. */
    public static StrategyAgent createWithOwnBuffers(
            String className,
            int strategyId,
            PositionView positionView,
            SecurityMaster securityMaster,
            Map<String, Object> strategyArgs) {
        GlobalSequence sequence = new GlobalSequence();
        return create(
                className,
                strategyId,
                new SequencedRingBuffer<>(Mbp10Schema::new, sequence),
                new SequencedRingBuffer<>(OrderExecutionReport::new, sequence),
                new SequencedRingBuffer<>(Intent::new, sequence),
                positionView,
                securityMaster,
                strategyArgs);
    }

    /** The constructor's arguments, or null if it does not take the infrastructure and exactly these arguments. */
    private static Object[] matchArguments(
            Constructor<?> ctor,
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            Map<String, Object> strategyArgs) {
        Parameter[] params = ctor.getParameters();
        if (params.length - INFRASTRUCTURE_PARAMS != strategyArgs.size() || !takesInfrastructure(params)) {
            return null;
        }
        Set<String> names = new HashSet<>();
        for (int i = INFRASTRUCTURE_PARAMS; i < params.length; i++) {
            names.add(params[i].getName());
        }
        if (!names.equals(strategyArgs.keySet())) {
            return null;
        }
        Object[] args = new Object[params.length];
        args[0] = strategyId;
        args[1] = marketDataBuffer;
        args[2] = execReportBuffer;
        args[3] = intentBuffer;
        args[4] = positionView;
        args[5] = securityMaster;
        for (int i = INFRASTRUCTURE_PARAMS; i < params.length; i++) {
            args[i] = convert(strategyArgs.get(params[i].getName()), params[i]);
        }
        return args;
    }

    private static boolean takesInfrastructure(Parameter[] params) {
        return params.length >= INFRASTRUCTURE_PARAMS
                && params[0].getType() == int.class
                && SequencedRingBuffer.class.isAssignableFrom(params[1].getType())
                && SequencedRingBuffer.class.isAssignableFrom(params[2].getType())
                && SequencedRingBuffer.class.isAssignableFrom(params[3].getType())
                && PositionView.class.isAssignableFrom(params[4].getType())
                && SecurityMaster.class.isAssignableFrom(params[5].getType());
    }

    private static Object convert(Object value, Parameter param) {
        Class<?> type = param.getType();
        if (type.isInstance(value)) {
            return value;
        }
        if (value instanceof Number number) {
            return convertNumber(number, type);
        }
        if (type == String.class) {
            return String.valueOf(value);
        }
        JavaType javaType = MAPPER.getTypeFactory().constructType(param.getParameterizedType());
        return MAPPER.convertValue(value, javaType);
    }

    private static Object convertNumber(Number number, Class<?> type) {
        if (type == int.class || type == Integer.class) {
            return number.intValue();
        }
        if (type == long.class || type == Long.class) {
            return number.longValue();
        }
        if (type == double.class || type == Double.class) {
            return number.doubleValue();
        }
        if (type == float.class || type == Float.class) {
            return number.floatValue();
        }
        throw new IllegalArgumentException("Cannot convert a number to " + type.getName());
    }
}
