# Antithesis harness images for the ArangoDB Agency.
# Build context: the antithesis/ directory (see config/docker-compose.yaml).

# ---- agent: stock 3.12.12 arangod, run as a standalone agency member ----
FROM arangodb/enterprise:3.12.12 AS agent
ENV NO_COLOR=1
# Same shared secret as K's demo. The workload signs its JWT with it.
RUN mkdir -p /etc/arangodb-agency /var/lib/arangodb-agency \
 && printf 'hello world' > /etc/arangodb-agency/jwt.secret
COPY images/agent/entrypoint.sh /usr/local/bin/agency-agent.sh
RUN chmod +x /usr/local/bin/agency-agent.sh
EXPOSE 8531
ENTRYPOINT ["/usr/local/bin/agency-agent.sh"]

# ---- workload: Python client with the Antithesis SDK ----
FROM debian:stable-slim AS workload
ENV NO_COLOR=1 PYTHONUNBUFFERED=1 PATH=/opt/venv/bin:$PATH
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/venv \
 && pip install --no-cache-dir antithesis==0.4.0 requests==2.32.3
COPY workload/ /opt/agency-workload/
COPY test/ /opt/antithesis/test/
RUN mkdir -p /opt/antithesis/catalog \
 && ln -s /opt/agency-workload /opt/antithesis/catalog/agency-workload
ENTRYPOINT ["python3", "/opt/agency-workload/entrypoint.py"]
