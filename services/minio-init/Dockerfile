FROM minio/mc:latest

COPY create-buckets.sh /scripts/create-buckets.sh
RUN chmod +x /scripts/create-buckets.sh

ENTRYPOINT ["/scripts/create-buckets.sh"]