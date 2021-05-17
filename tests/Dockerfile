FROM python:3.8-slim
LABEL maintainer="cash.costello@jhuapl.edu"
LABEL description="Image for running Patapsco unit tests"

# workaround for https://github.com/debuerreotype/docker-debian-artifacts/issues/24
RUN mkdir -p /usr/share/man/man1

RUN apt-get update && apt-get install -y gcc g++ default-jdk maven \
    && pip install stanza \
    && python -c 'import stanza; stanza.download("ar")' \
    && python -c 'import stanza; stanza.download("en")' \
    && python -c 'import stanza; stanza.download("fa")' \
    && python -c 'import stanza; stanza.download("ru")'

RUN useradd -ms /bin/bash runner
USER runner
WORKDIR /home/runner
ENV PATH="/home/runner/.local/bin:${PATH}"
