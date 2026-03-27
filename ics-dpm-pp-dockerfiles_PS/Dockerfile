#FROM openjdk:17
FROM amazoncorretto:17.0.15-al2023

ARG GIT_SHA="undefined"

LABEL Description="This is the Docker image of messages producer to kafka topic" \
      Maintainer="Ramkumar Sundaram" \
      Maintainer_Email="ramkumar.sundaram@broadridge.com" \
      Business_Unit="ICS-BRCC" \
      Git_SHA="${GIT_SHA}" \
      Product="DPM"

# Set the working directory in the container
WORKDIR /dpm-app

ENV KEYSTORE_PATH=/dpm-app/dpm-keystore.p12
ENV KEYSTORE_PASS=changeit
ENV KEY_ALIAS=dpm-ssl

# Update package list and upgrade openssl
# RUN yum update -y && \
#     yum install -y openssl-1:3.2.2-6.el9_5.1 && \
#     yum clean all

# Generate the keystore and certificate
RUN keytool -genkeypair -alias $KEY_ALIAS -keyalg RSA -keysize 2048 -keystore $KEYSTORE_PATH -storepass $KEYSTORE_PASS -keypass $KEYSTORE_PASS -validity 365 -dname "CN=dpm, OU=IT, O=Broadridge, L=Toronto, S=ON, C=CA"

# Optional: Export the certificate (if needed)
# RUN keytool -export -alias $KEY_ALIAS -keystore $KEYSTORE_PATH -file /dpm-app/certificate.cer -storepass $KEYSTORE_PASS

# Command to run on container start (optional)
CMD ["echo", "Keystore generated successfully at $KEYSTORE_PATH"]

COPY ./target/*.jar dpm-kafka-producer.jar
# Make port 8080 available to the world outside this container
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "dpm-kafka-producer.jar"]
