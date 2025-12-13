# Longbow

[![CI](https://github.com/23skdu/longbow/actions/workflows/ci.yml/badge.svg)](https://github.com/23skdu/longbow/actions/workflows/ci.yml)
[![Helm Validation](https://github.com/23skdu/longbow/actions/workflows/helm-validation.yml/badge.svg)](https://github.com/23skdu/longbow/actions/workflows/helm-validation.yml)
[![Markdown Lint](https://github.com/23skdu/longbow/actions/workflows/markdown-lint.yml/badge.svg)](https://github.com/23skdu/longbow/actions/workflows/markdown-lint.yml)
[![Release](https://github.com/23skdu/longbow/actions/workflows/release.yml/badge.svg)](https://github.com/23skdu/longbow/actions/workflows/release.yml)
<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/fc6c1662-f529-424a-a0d6-e7e27e813592" />

**Longbow** is a high-performance, in-memory vector store implementing the **Apache Arrow Flight** protocol. It is designed for efficient, zero-copy transport of large datasets and vector embeddings between agents and services.

## Features

*   **Protocol**: Apache Arrow Flight (over gRPC/HTTP2).
*   **Storage**: In-memory ephemeral storage for high-speed access.
*   **Observability**: Structured JSON logging.
*   **Deployment**: Minimal Docker image (scratch-based).

## Usage

### Running locally

```bash
go run cmd/longbow/main.go
```

### Docker

```bash
docker build -t longbow .
docker run -p 3000:3000 longbow
```
