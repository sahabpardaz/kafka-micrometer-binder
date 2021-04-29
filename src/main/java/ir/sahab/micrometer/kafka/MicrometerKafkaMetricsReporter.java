package ir.sahab.micrometer.kafka;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports kafka consumer and producer metrics to Micrometer.
 * To use this class you should set {@link CommonClientConfigs#METRIC_REPORTER_CLASSES_CONFIG} to fully qualified
 * name of this class. Also you must set {@link CommonClientConfigs#CLIENT_ID_CONFIG} to a unique value in JVM.
 *
 * @see org.apache.kafka.common.metrics.JmxReporter
 * @see io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
 */
public class MicrometerKafkaMetricsReporter implements MetricsReporter {
    private static final Logger logger = LoggerFactory.getLogger(MicrometerKafkaMetricsReporter.class);

    private static final String METRICS_PREFIX = "kafka.";
    private static final CompositeMeterRegistry registry = Metrics.globalRegistry;

    // Methods of this class are called by different kafka threads so we use lock for thread-safety.
    private static final Object LOCK = new Object();

    /**
     * Kafka provides same metric name with multiple tags for example
     * it provides both 'fetch.ms{id="client1"}' and 'fetch.ms{id="client1", partition="p1"}' but prometheus requires
     * that a metric name always have same set of tags. Because of this we only keep metrics with higher number of
     * tags (which are more precise) and we ignore metric names with lower number of tags.
     * This map saves metric names and highest number of tags seen already for each one.
     */
    private static final Map<String, Integer> currentMetrics = new HashMap<>();

    /**
     * By default kafka add clientId tag to all metrics. we save cleintId so we can filter metrics by it later.
     * If user don't specify the clientId, Kafka will automatically assign a clientId to it which will make our
     * metric tags non-deterministic and absolutely is not intended.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        if (!configs.containsKey(CommonClientConfigs.CLIENT_ID_CONFIG)) {
            throw new IllegalArgumentException(CommonClientConfigs.CLIENT_ID_CONFIG + " must be specified");
        }
        String clientId = (String) configs.get(CommonClientConfigs.CLIENT_ID_CONFIG);
        if (clientId.trim().isEmpty()) {
            throw new IllegalArgumentException(CommonClientConfigs.CLIENT_ID_CONFIG + " can't be empty");
        }
        logger.info("Kafka metric exporter for client {} started", clientId);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        for (KafkaMetric metric : metrics) {
            metricChange(metric);
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        // Ignore deprecated old metrics and non-numeric metrics
        if (metric.metricName().description().contains("DEPRECATED") || !(metric.metricValue() instanceof Number)) {
            return;
        }

        synchronized (LOCK) {
            addMetric(metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (LOCK) {
            final String metericName = meterName(metric);
            if (metric.metricName().tags().size() == currentMetrics.getOrDefault(metericName, -1)) {
                registry.find(metericName).tags(meterTags(metric)).meters().forEach(registry::remove);
                logger.trace("Kafka metric {} with tags {} removed from registry",
                        metericName, metric.metricName().tags());
            }
        }
    }

    /**
     * Registers given kafka metric to Micrometer.
     * If a metric with same name but higher number of tags seen before, given metric will be ignored.
     * If given metric has more tags than previously seen metrics with same name, all of metrics with that name
     * will be removed from registry and the given one will be registered.
     * After some time, our {@link #currentMetrics} will converge to stable values and is not changed afterwards.
     * @param metric KafkaMetric to add
     */
    private static void addMetric(KafkaMetric metric) {
        final String metricName = meterName(metric);
        final Map<String, String> metricTags = metric.metricName().tags();
        if (currentMetrics.containsKey(metricName)) {
            Integer currentNumberOfTags = currentMetrics.get(metricName);
            if (metricTags.size() < currentNumberOfTags) {
                logger.trace("Kafka metric {} with tags {} ignored.", metricName, metricTags);
                return;
            }

            // Remove previously added metric with less tags.
            if (metricTags.size() > currentNumberOfTags) {
                registry.find(metricName).meters().forEach(registry::remove);
            }
        }
        bindMeter(metric, metricName, meterTags(metric));
        currentMetrics.put(metricName, metricTags.size());
    }

    /**
     * Binds given metric to Micrometer registry.
     */
    private static void bindMeter(KafkaMetric metric, String name, Iterable<Tag> tags) {
        logger.trace("Kafka metric {} with tags {} bound to registry", name, tags);
        registerGauge(metric, name, tags);
    }

    private static void registerGauge(Metric metric, String name, Iterable<Tag> tags) {
        Gauge.builder(name, metric, MicrometerKafkaMetricsReporter::getMetricValue)
                .tags(tags)
                .description(metric.metricName().description())
                .register(registry);
    }

    private static double getMetricValue(Metric metric) {
        return ((Number) metric.metricValue()).doubleValue();
    }

    private static List<Tag> meterTags(Metric metric) {
        return metric.metricName().tags().entrySet().stream()
                .map(e -> Tag.of(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Creates a metric name based on given Kafka metric.
     * Kafka metric names are hard-coded and distributed in kafka client code-base. This function is tested with
     * Kafka 1.1.1 and changing client version may or may not change metric names and their convention.
     */
    private static String meterName(Metric metric) {
        String name = METRICS_PREFIX + metric.metricName().group() + "." + metric.metricName().name();
        return name.replace("-metrics", "").replace("-", ".");
    }

    @Override
    public void close() {
        // There is nothing to close
    }
}
