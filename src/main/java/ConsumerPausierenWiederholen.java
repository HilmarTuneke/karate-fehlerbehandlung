import io.smallrye.faulttolerance.api.ExponentialBackoff;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ConsumerPausierenWiederholen {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerPausierenWiederholen.class);

    @Inject
    Controller controller;

    @Incoming("kafka")
    @Retry(delay = 500L, jitter = 500L, maxRetries = -1, maxDuration = 0)
    @ExponentialBackoff(maxDelay = 2, maxDelayUnit = ChronoUnit.HOURS)
    public CompletionStage<Void> consume(KafkaRecord<String, String> record) {
        try {
            controller.process(record.getPayload());
            return record.ack();
        } catch (Exception e) {
            LOGGER.error("Oops, something went terribly wrong with " + record, e);
            throw e;
        }
    }

}
