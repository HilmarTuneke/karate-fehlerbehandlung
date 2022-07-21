import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ConsumerHerunterfahrenMitSkip {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerHerunterfahrenMitSkip.class);

    @Inject
    Controller controller;

    @Inject
    @Channel("dead-letter-queue")
    Emitter<String> deadLetterEmitter;

    @Incoming("kafka")
    public CompletionStage<Void> consume(IncomingKafkaRecord<String, String> record) {
        if(controller.shouldSkip(record.getPartition(), record.getOffset())) {
            CompletionStage<Void> returnValue = record.ack();
            controller.notifySkip(record.getPartition(), record.getOffset());
            return returnValue;
        } else {
            try {
                controller.process(record.getPayload());
                return record.ack();
            } catch (Exception e) {
                LOGGER.error("Oops, something went terribly wrong with " + record, e);
                System.exit(1);
                return null; // never reached
            }
        }
    }
}
