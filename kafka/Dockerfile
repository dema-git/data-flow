FROM amazoncorretto:17

WORKDIR /app

RUN yum install -y nc && yum clean all

COPY kafdrop/kafdrop.jar ./kafdrop.jar
COPY scripts/wait-for-it.sh ./wait-for-it.sh
RUN chmod +x ./wait-for-it.sh
