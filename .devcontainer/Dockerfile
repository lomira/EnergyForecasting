FROM nvidia/cuda:13.2.1-runtime-ubuntu24.04

RUN apt-get update && apt-get install -y \
    curl \
    git \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*


COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv python install 3.14

WORKDIR /workspaces/energyforecasting

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON=3.14
ENV UV_PYTHON_DOWNLOADS=never