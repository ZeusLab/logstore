FROM alpine:latest

USER root
RUN mkdir -p /hermes \
	&& mkdir -p /hermes/bin

COPY entrypoint.sh /hermes/bin/entrypoint.sh
COPY hermes /hermes/bin/hermes

EXPOSE 80

ENV PATH="/hermes/bin:${PATH}"
ENTRYPOINT ["/hermes/bin/entrypoint.sh"]
CMD ["hermes"]
