# Error Handling

### Dead Letter Queue (DLQ)

```python
class DeadLetterQueue:
    """Handle failed records with dead letter queue pattern."""

    def __init__(self, dlq_topic: str, producer: KafkaProducer):
        self.dlq_topic = dlq_topic
        self.producer = producer

    def send_to_dlq(self, record, error: Exception, context: dict):
        """Send failed record to DLQ with error metadata."""

        dlq_record = {
            "original_record": record,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.utcnow().isoformat(),
            "context": context,
            "retry_count": context.get("retry_count", 0)
        }

        self.producer.send(
            self.dlq_topic,
            value=json.dumps(dlq_record).encode('utf-8')
        )

def process_with_dlq(consumer, processor, dlq):
    """Process records with DLQ for failures."""

    for message in consumer:
        try:
            result = processor.process(message.value)
            # Success - commit offset
            consumer.commit()

        except ValidationError as e:
            # Non-retryable - send to DLQ immediately
            dlq.send_to_dlq(
                message.value,
                e,
                {"topic": message.topic, "partition": message.partition}
            )
            consumer.commit()  # Don't retry

        except TemporaryError as e:
            # Retryable - don't commit, let consumer retry
            # After max retries, send to DLQ
            retry_count = message.headers.get("retry_count", 0)
            if retry_count >= MAX_RETRIES:
                dlq.send_to_dlq(message.value, e, {"retry_count": retry_count})
                consumer.commit()
            else:
                raise  # Will be retried
```

### Circuit Breaker

```python
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import threading

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if recovered

@dataclass
class CircuitBreaker:
    """Circuit breaker for external service calls."""

    failure_threshold: int = 5
    recovery_timeout: timedelta = timedelta(seconds=30)
    success_threshold: int = 3

    def __post_init__(self):
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""

        with self.lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                else:
                    raise CircuitOpenError("Circuit is open")

        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result

        except Exception as e:
            self._record_failure()
            raise

    def _record_success(self):
        with self.lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0

    def _record_failure(self):
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()

            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.success_count = 0
            elif self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN

    def _should_attempt_reset(self):
        if self.last_failure_time is None:
            return True
        return datetime.now() - self.last_failure_time >= self.recovery_timeout

# Usage
circuit = CircuitBreaker(failure_threshold=5, recovery_timeout=timedelta(seconds=60))

def call_external_api(data):
    return circuit.call(external_api.process, data)
```

---
