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
public class ConsumerRetry {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRetry.class);

    @Inject
    Controller controller;

    // TODO: Wie sähe das mit der Retry-Strategie aus?
    @Incoming("kafka")
    @Retry(maxRetries = -1, delay = 2, delayUnit = ChronoUnit.MINUTES, jitter = 500L)
    public CompletionStage<Void> consume(KafkaRecord<String, String> record) {
        try {
            controller.process(record.getPayload());
        } catch (Exception e) {
            LOGGER.error("Oops, something went terribly wrong with " + record, e);
            throw e;
        }
        return record.ack();
    }

}
