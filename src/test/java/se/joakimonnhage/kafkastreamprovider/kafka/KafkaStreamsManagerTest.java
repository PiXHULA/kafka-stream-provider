package se.joakimonnhage.kafkastreamprovider.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import se.joakimonnhage.kafkastreamprovider.avro.Message;
import se.joakimonnhage.kafkastreamprovider.dto.StoredMessage;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests driving {@link KafkaStreamsManager} without a live Kafka broker, by injecting
 * mocked {@link KafkaProducer}/{@link KafkaStreams} instances directly into its private fields
 * (bypassing CDI and {@code @PostConstruct}).
 */
@ExtendWith(MockitoExtension.class)
class KafkaStreamsManagerTest {

    @Mock
    private KafkaProducer<String, Message> producer;

    @Mock
    private KafkaStreams streams;

    @Mock
    private ReadOnlyKeyValueStore<String, Message> store;

    @Captor
    ArgumentCaptor<ProducerRecord<String, Message>> captor;

    private KafkaStreamsManager manager;

    @BeforeEach
    void setUp() throws Exception {
        manager = new KafkaStreamsManager();
        manager.topic = "messages";
        setPrivateField(manager, "producer", producer);
        setPrivateField(manager, "streams", streams);
    }

    @Test
    void produceSendsAvroMessageBuiltFromTextAndFlushes() {
        manager.produce("hello world");

        verify(producer).send(captor.capture(), any());
        verify(producer).flush();

        ProducerRecord<String, Message> record = captor.getValue();
        assertEquals("messages", record.topic());
        assertNotNull(record.key());

        Message value = record.value();
        assertEquals(record.key(), value.getId().toString());
        assertEquals("hello world", value.getMessage().toString());
        assertTrue(value.getTimestamp() > 0);
    }

    @Test
    void produceWrapsSynchronousSendFailureInRuntimeException() {
        when(producer.send(any(), any())).thenThrow(new KafkaException("boom"));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> manager.produce("hello"));
        assertTrue(ex.getMessage().contains("messages"));
        assertInstanceOf(KafkaException.class, ex.getCause());
    }

    @Test
    void readAllReturnsEveryEntryFromTheStateStore() {
        Message m1 = Message.newBuilder().setId("id-1").setTimestamp(1L).setMessage("first").build();
        Message m2 = Message.newBuilder().setId("id-2").setTimestamp(2L).setMessage("second").build();

        KeyValueIterator<String, Message> iterator = mockIterator(List.of(
                new KeyValue<>("key-1", m1),
                new KeyValue<>("key-2", m2)
        ));

        when(streams.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);

        List<StoredMessage> result = manager.readAll();

        assertEquals(2, result.size());
        assertEquals("key-1", result.getFirst().key);
        assertEquals("id-1", result.getFirst().id);
        assertEquals(1L, result.getFirst().timestamp);
        assertEquals("first", result.getFirst().message);
        assertEquals("key-2", result.get(1).key);
        assertEquals("id-2", result.get(1).id);
        assertEquals(2L, result.get(1).timestamp);
        assertEquals("second", result.get(1).message);
    }

    @Test
    void readAllReturnsEmptyListWhenStoreIsEmpty() {
        KeyValueIterator<String, Message> iterator = mockIterator(List.of());

        when(streams.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);

        assertTrue(manager.readAll().isEmpty());
    }

    @Test
    void resolveStateDirReturnsValueUnchangedWhenNoPlaceholderPresent() throws Exception {
        Object result = invokeResolveStateDir("/absolute/state-dir");
        assertEquals("/absolute/state-dir", result);
    }

    @Test
    void resolveStateDirReturnsNullUnchanged() throws Exception {
        assertNull(invokeResolveStateDir(null));
    }

    @Test
    void resolveStateDirThrowsWhenPlaceholderUnresolvedAndNoEnvOverride() {
        // Only meaningful when the test JVM doesn't itself have KAFKA_STATE_DIR set.
        assumeTrue(System.getenv("KAFKA_STATE_DIR") == null);

        InvocationTargetException wrapper = assertThrows(InvocationTargetException.class,
                () -> invokeResolveStateDir("${project.basedir}/kafka-streams-state"));

        assertInstanceOf(IllegalStateException.class, wrapper.getCause());
        assertTrue(wrapper.getCause().getMessage().contains("${project.basedir}"));
    }

    /**
     * Builds a mocked {@link KeyValueIterator} that yields the given entries in order, so tests
     * don't need a hand-written anonymous/delegating iterator implementation just to drive
     * {@code store.all()}.
     */
    @SuppressWarnings("unchecked")
    private static KeyValueIterator<String, Message> mockIterator(List<KeyValue<String, Message>> entries) {
        KeyValueIterator<String, Message> iterator = mock(KeyValueIterator.class);

        Boolean[] hasNextAnswers = new Boolean[entries.size() + 1];
        Arrays.fill(hasNextAnswers, Boolean.TRUE);
        hasNextAnswers[entries.size()] = Boolean.FALSE;
        when(iterator.hasNext()).thenReturn(hasNextAnswers[0], Arrays.copyOfRange(hasNextAnswers, 1, hasNextAnswers.length));

        if (!entries.isEmpty()) {
            KeyValue<String, Message>[] rest = entries.subList(1, entries.size()).toArray(new KeyValue[0]);
            when(iterator.next()).thenReturn(entries.get(0), rest);
        }

        return iterator;
    }

    private static Object invokeResolveStateDir(String configuredStateDir) throws Exception {
        Method method = KafkaStreamsManager.class.getDeclaredMethod("resolveStateDir", String.class);
        method.setAccessible(true);
        return method.invoke(null, (Object) configuredStateDir);
    }

    private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = KafkaStreamsManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}

