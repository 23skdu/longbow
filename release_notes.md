## 🚀 v0.0.4: Split-Port Architecture

This release introduces a major architectural improvement to separate data traffic from metadata operations, ensuring high availability even under heavy load.

### ✨ New Features
- **Split Port Architecture**:
  - **Data Server (Port 3000)**: Dedicated to DoGet and DoPut operations.
  - **Meta Server (Port 3001)**: Dedicated to ListFlights and GetFlightInfo.
- **Helm Chart**: Updated to support the new metaPort configuration and environment variables.

### 📚 Documentation
- Updated README.md and docs/components.md with architecture details.
- Updated Docker usage examples.

### 🐳 Docker Images
- ghcr.io/23skdu/longbow:0.0.4
- ghcr.io/23skdu/longbow:latest
