# Longbow

![CI](https://github.com/23skdu/longbow/actions/workflows/ci.yml/badge.svg)
![Helm Validation](https://github.com/23skdu/longbow/actions/workflows/helm-validation.yml/badge.svg)
![Markdown Lint](https://github.com/23skdu/longbow/actions/workflows/markdown-lint.yml/badge.svg)

![image](https://github.com/user-attachments/assets/775eb0b4-7e55-4524-abda-c9489de0194e)

Longbow es un motor de vectores distribuido y de alto rendimiento construido para cargas de trabajo modernas de IA/Agentes. Aprovecha rutas de datos zero-copy, optimizaciones SIMD y backends de almacenamiento avanzados para ofrecer una latencia inferior al milisegundo.

## Características Principales

- **Alto Rendimiento**: Basado en Apache Arrow para transferencias de datos zero-copy.
- **Distribuido**: Hashing consistente y membresía basada en gossip (protocolo SWIM).
- **Almacenamiento Optimizado**: Backend WAL opcional con `io_uring` para una ingesta de alto rendimiento.
- **Conciencia de Hardware**: Asignación de memoria consciente de NUMA y cálculos de distancia vectorial SIMD.
- **Cliente Inteligente**: SDK de Go resiliente que gestiona el enrutamiento de solicitudes de forma transparente.

## Arquitectura

Longbow utiliza una arquitectura shared-nothing donde cada nodo está débilmente acoplado.

Consulta la [Guía de Arquitectura](docs/architecture.md) para un análisis profundo.

## Primeros Pasos

### Requisitos Previos

- Go 1.25+
- Linux (recomendado para el mejor rendimiento) o macOS

### Instalación

```bash
git clone https://github.com/23skdu/longbow.git
cd longbow
go build -o bin/longbow ./cmd/longbow
```

### Ejecutar un Clúster Local

```bash
./scripts/start_local_cluster.sh
```

### Ejecutar Benchmarks

Longbow incluye una suite de benchmarks exhaustiva y multi-plataforma:

```bash
# Ejecutar un benchmark específico para tipos y dimensiones determinadas
python3 scripts/unified_benchmark.py --modes cpu,metal --dtypes float32,turboquant --dims 128,384,768

# Los resultados se generan como una matriz de Markdown en docs/performance.md
```

### Distribución de Binarios para GPU

Longbow se compila nativamente para arquitecturas de CPU por defecto. Para habilitar la aceleración por GPU, debes compilar o ejecutar los binarios específicos de la plataforma:

- **macOS (Metal)**: Construye el binario universal mediante `make build-darwin-universal`, que utiliza `lipo` para crear un binario "fat" que contiene objetivos tanto para `x86_64` (CPU) como `arm64` (Metal).
- **Linux (CUDA)**: Construye el binario habilitado para CUDA mediante `make build-cuda` (requiere NVIDIA toolkit).

**Nota:** Si Longbow se inicia en hardware compatible con GPU sin el binario habilitado para GPU, volverá silenciosamente a la ejecución por CPU y mostrará una advertencia de inicio de 3 segundos.

## Configuración

Longbow se configura mediante variables de entorno. Los límites clave incluyen:

- `LONGBOW_MAX_MEMORY` (Límite blando con contrapresión exponencial)
- `LONGBOW_MAX_MEMORY_HARD` (Techo duro absoluto que dispara el rechazo `ResourceExhausted`)

Consulta [Despliegue y Configuración](docs/deploy.md#2-configuration) y [Límites](docs/limits.md) para más detalles.

Flags notables:

- `STORAGE_USE_IOURING=true` (Habilita el nuevo motor de almacenamiento de Linux)
- `LONGBOW_HNSW_TURBOQUANT_ENABLED=true` (Habilita empaquetado de bits acelerado por SIMD)
- `LONGBOW_LEARNED_INDEX_ENABLED=true` (Habilita enrutamiento adaptativo por índice aprendido)

- **Protocolo**: Apache Arrow Flight (sobre gRPC/HTTP2).
- **Búsqueda**: Búsqueda vectorial HNSW de alto rendimiento con soporte híbrido (Denso + Disperso) y tipos de vectores polimórficos.
- **Métricas de Distancia**: Métricas enchufables (Euclidiana, Coseno, Producto Punto) con optimizaciones SIMD para todos los tipos soportados.
- **Filtrado**: Filtrado de predicados consciente de metadatos para búsquedas y escaneos.
- **Ciclo de Vida**: Soporte para eliminación de vectores mediante tombstones.
- **Durabilidad**: WAL con snapshots en formato Apache Parquet.
- **Almacenamiento**: Almacenamiento efímero en memoria para acceso de alta velocidad zero-copy.
- **Observabilidad**: Registro JSON estructurado y más de 100 métricas de Prometheus.

## Tipos de Datos y Dimensiones Soportadas

Longbow soporta los siguientes tipos de datos vectoriales con núcleos SIMD optimizados:

| Tipo de Dato | Dimensiones Soportadas | Notas |
| --------- | -------------------- | ----- |
| **float32** | 128 - 3072 | Optimización SIMD completa (AVX2/AVX-512/Neon) |
| **float16** | 128 - 3072 | Núcleos GPU Metal/CUDA + fallback a CPU |
| **float64** | 128 - 3072 | Optimización SIMD completa |
| **int8/uint8** | 128 - 3072 | Optimizado para AVX2/Neon |
| **int16/uint16** | 128 - 3072 | Optimizado para AVX2/Neon |
| **int32/uint32** | 128 - 3072 | Optimizado para AVX2/Neon |
| **int64/uint64** | 128 - 3072 | SIMD genérico y soporte de filtro de metadatos |
| **complex64/128** | 128 - 3072 | Optimización SIMD completa |
| **turboquant** | 128 - 3072 | Optimizado para FWHT NEON/AVX2 |

### Filtrado de Metadatos Acelerado por SIMD

A partir de la versión 0.1.9, Longbow soporta **filtrado de predicados acelerado por SIMD** dentro de la ruta de búsqueda HNSW. Esto permite QPS extremadamente altos en búsquedas filtradas al desplazar la lógica booleana hacia el bucle de recorrido vectorial.

- **Ops Soportadas**: `=`, `!=`, `>`, `>=`, `<`, `<=`
- **Optimizaciones**: Núcleos especializados AVX-512 (AMD64) y Neon (ARM64).
- **Impacto**: Hasta 5 veces más QPS para filtros altamente selectivos.

### Dimensiones de Núcleo Optimizadas

Las siguientes dimensiones cuentan con núcleos optimizados específicos para la dimensión:

| Dimensión | Tamaño de Bloque | Optimización |
| --------- | ---------- | ------------ |
| 128 | N/A | Desenrollado SIMD directo |
| 256 | N/A | Desenrollado SIMD directo |
| 384 | N/A | Núcleos específicos AVX2/NEON |
| 768 | 256 | SIMD bloqueado |
| 1024 | 256 | SIMD bloqueado |
| 1536 | 256 | SIMD bloqueado |
| 2048 | 512 | SIMD bloqueado |
| 3072 | 512 | SIMD bloqueado |

## Arquitectura y Puertos

Para asegurar un alto rendimiento bajo carga, Longbow divide el tráfico en dos servidores gRPC dedicados:

- **Servidor de Datos (Puerto 3000)**: Gestiona operaciones pesadas de E/S (`DoGet`, `DoPut`, `DoExchange`).
- **Servidor de Meta (Puerto 3001)**: Gestiona operaciones ligeras de metadatos (`ListFlights`, `GetFlightInfo`, `DoAction`).

**¿Por qué?**
La separación de estas preocupaciones evita que las operaciones de transferencia de datos de larga duración bloqueen las solicitudes de metadatos. Esto asegura que los clientes siempre puedan descubrir flujos y verificar el estado, incluso cuando el sistema esté bajo una fuerte carga de lectura/escritura.

## Observabilidad y Métricas

Longbow expone métricas de Prometheus en un puerto dedicado para asegurar la observabilidad sin impactar el servicio principal de Flight.

- **Puerto de Scrape**: 9090
- **Ruta de Scrape**: /metrics

### Métricas Personalizadas

### Métricas Clave

| Nombre de la Métrica | Tipo | Descripción |
| :--- | :--- | :--- |
| `longbow_flight_ops_total` | Counter | Número total de operaciones de Flight (DoGet, DoPut, etc.) |
| `longbow_flight_duration_seconds` | Histogram | Distribución de latencia de las operaciones de Flight |
| `longbow_flight_rows_processed_total` | Counter | Total de filas procesadas en escaneos y búsquedas |
| `longbow_hnsw_search_duration_seconds` | Histogram | Latencia de las operaciones de búsqueda k-NN |
| `longbow_hnsw_node_count` | Gauge | Número actual de vectores en el índice |
| `longbow_tombstones_total` | Gauge | Número de tombstones de vectores eliminados activos |
| `longbow_index_queue_depth` | Gauge | Profundidad de la cola de indexación asíncrona |
| `longbow_memory_fragmentation_ratio` | Gauge | Relación entre memoria del sistema reservada vs usada |
| `longbow_wal_bytes_written_total` | Counter | Total de bytes escritos en el WAL |
| `longbow_snapshot_duration_seconds` | Histogram | Duración del proceso de snapshot de Parquet |
| `longbow_evictions_total` | Counter | Número total de registros expulsados (LRU) |
| `longbow_ipc_decode_errors_total` | Counter | Recuento de errores de decodificación IPC o panics |

Para una explicación detallada de las más de 100 métricas, consulta la [Documentación de Métricas](docs/metrics.md).

También se exponen las métricas estándar del runtime de Go.

## Uso

### Ejecución local

```bash
go run cmd/longbow/main.go
```

### Docker

```bash
docker build -t longbow .
docker run -p 3000:3000 -p 3001:3001 -p 9090:9090 longbow
```

## Documentación

- [Métricas y Funciones de Distancia](docs/functions.md)
- [Despliegue y Configuración](docs/deploy.md)
- [Benchmarks de Rendimiento](docs/performance.md)
- [Persistencia y Snapshots](docs/persistence.md)
- [Arquitectura de Búsqueda Vectorial](docs/vectorsearch.md)
- [Guía de Solución de Problemas](docs/troubleshooting.md)
- [Documentación de Métricas](docs/metrics.md)
