# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Copy the executable JAR file into the container
COPY target/min-max-cache-2.0.jar min-max-cache-2.0.jar

# Expose the port your Spring Boot app runs on
EXPOSE 8080

# Set the command to run the JAR file
ENTRYPOINT ["java", "-jar", "min-max-cache-2.0.jar"]