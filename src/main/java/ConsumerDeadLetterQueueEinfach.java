import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ConsumerDeadLetterQueueEinfach {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDeadLetterQueueEinfach.class);

    @Inject
    Controller controller;

    @Inject
    @Channel("dead-letter-queue")
    Emitter<String> deadLetterEmitter;

    @Incoming("kafka")
    public CompletionStage<Void> consume(KafkaRecord<String, String> record) {
        try {
            controller.process(record.getPayload());
        } catch (Exception e) {
            LOGGER.error("Oops, something went terribly wrong with " + record.toString(), e);
            deadLetterEmitter.send(record);
        }
        return record.ack();
    }

}
