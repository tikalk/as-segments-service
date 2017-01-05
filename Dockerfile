###
# vert.x docker device-manager using a Java verticle packaged as a fatjar
# To build:
#  docker build -t fleettracker/as-segment-service .
# To run:
#   docker run -t -i -p 8080:8080 fleettracker/as-segment-service
###

FROM java:8

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

EXPOSE 9080

# Copy your fat jar to the container
ADD build/distributions/as-segment-service-3.1.0.tar /segment-service

# Launch the verticle
WORKDIR /segment-service
CMD ./segment-service.sh
