package br.com.motorfraude.engine.adapters.gateway.rules;

import br.com.motorfraude.engine.application.gateway.RuleSetMetrics;
import br.com.motorfraude.engine.application.gateway.RuleSetProvider;
import br.com.motorfraude.rules.kernel.compiler.CompiledRuleSet;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaRuleSetProvider implements RuleSetProvider, AutoCloseable {

    public static final String TOPIC = "rule-sets.v1";
    private static final Duration POLL = Duration.ofMillis(500);
    private static final Duration BACKOFF = Duration.ofSeconds(2);
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRuleSetProvider.class);

    private final Consumer<String, String> consumer;
    private final String topic;
    private final TopicPartition partition;
    private final RuleSetLoader loader;
    private final RuleSetMetrics metrics;
    private volatile boolean running;
    private volatile boolean caughtUp;
    private boolean initialized;
    private long targetOffset;
    private Thread thread;

    public KafkaRuleSetProvider(Consumer<String, String> consumer, String topic, RuleSetLoader loader, RuleSetMetrics metrics) {
        this.consumer = consumer;
        this.topic = topic;
        this.partition = new TopicPartition(topic, 0);
        this.loader = loader;
        this.metrics = metrics;
    }

    @Override
    public Optional<CompiledRuleSet> current() {
        return loader.current();
    }

    public boolean ready() {
        return caughtUp && loader.current().isPresent();
    }

    public boolean running() {
        return running;
    }

    public void initialize() {
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
        targetOffset = consumer.endOffsets(List.of(partition)).getOrDefault(partition, 0L);
        caughtUp = targetOffset == 0;
        initialized = true;
    }

    public void pollOnce(Duration timeout) {
        try {
            for (var record : consumer.poll(timeout)) {
                loader.apply(record.value());
            }
            if (!caughtUp && consumer.position(partition) >= targetOffset) {
                caughtUp = true;
            }
        } catch (WakeupException e) {
            throw e;
        } catch (RuntimeException e) {
            metrics.failed("kafka");
            LOG.warn("falha ao ler {}; mantido o conjunto atual (versão {}): {}", topic, loader.version(), e.toString());
            pause(BACKOFF);
        }
    }

    public synchronized void start() {
        if (thread != null) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().name("rule-set-consumer").daemon().start(() -> {
            try {
                while (running) {
                    if (!initialized) {
                        initializeWithRetry();
                    } else {
                        pollOnce(POLL);
                    }
                }
            } catch (WakeupException ignored) {
                LOG.info("leitura do {} encerrada", topic);
            } finally {
                running = false;
                consumer.close();
            }
        });
    }

    @Override
    public synchronized void close() {
        if (thread == null) {
            running = false;
            return;
        }
        running = false;
        consumer.wakeup();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        thread = null;
    }

    private void initializeWithRetry() {
        try {
            initialize();
        } catch (WakeupException e) {
            throw e;
        } catch (RuntimeException e) {
            metrics.failed("kafka");
            LOG.warn("não foi possível iniciar a leitura de {}; tentando de novo: {}", topic, e.toString());
            pause(BACKOFF);
        }
    }

    private void pause(Duration duration) {
        try {
            Thread.sleep(running ? duration.toMillis() : 0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
