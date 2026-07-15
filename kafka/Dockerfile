FROM confluentinc/cp-kafka-connect:7.9.4

# Install Debezium MySQL, Postgres, dan JDBC connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:latest && \
    confluent-hub install --no-prompt debezium/debezium-connector-postgresql:latest && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
