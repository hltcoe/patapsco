FROM python:3.8-slim
LABEL maintainer="cash.costello@jhuapl.edu"
LABEL description="Image for running Patapsco unit tests"

RUN apt-get update && apt-get install -y gcc g++ \ 
    && pip install stanza \
    && python -c 'import stanza; stanza.download("ar")' \
    && python -c 'import stanza; stanza.download("en")' \
    && python -c 'import stanza; stanza.download("fa")' \
    && python -c 'import stanza; stanza.download("ru")'
