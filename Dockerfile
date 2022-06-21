FROM openjdk:11
ARG JAR_FILE=target/kafka-demo-1.0-SNAPSHOT.jar
COPY ${JAR_FILE} /kafka-demo-1.0-SNAPSHOT.jar
ENTRYPOINT exec java $JAVA_OPTS -jar /kafka-demo-1.0-SNAPSHOT.jar
