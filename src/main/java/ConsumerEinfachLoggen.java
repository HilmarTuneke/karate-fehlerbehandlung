import io.smallrye.reactive.messaging.kafka.KafkaMessage;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ConsumerEinfachLoggen {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerEinfachLoggen.class);

    @Inject
    Controller controller;

    @Incoming("kafka")
    public CompletionStage<Void> consume(KafkaRecord<String, String> record) {
        try {
            controller.process(record.getPayload());
        } catch (Exception e) {
            LOGGER.error("Oops, something went terribly wrong with " + record.toString(), e);
        }
        return record.ack();
    }

}
