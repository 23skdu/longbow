package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/parquet-go/parquet-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
)

// MediaModality enumerates supported multi-modal asset modalities.
type MediaModality string

const (
	ModalityImage MediaModality = "image"
	ModalityAudio MediaModality = "audio"
	ModalityText  MediaModality = "text"
)

// MultiModalItem represents one multi-modal record for embedding and Arrow storage.
type MultiModalItem struct {
	ID        string         `json:"id"`
	Modality  MediaModality  `json:"modality"`
	MimeType  string         `json:"mime_type,omitempty"`
	SourceURI string         `json:"source_uri,omitempty"`
	Caption   string         `json:"caption,omitempty"`
	Blob      []byte         `json:"blob,omitempty"`
	BlobB64   string         `json:"blob_base64,omitempty"`
	Vector    []float32      `json:"vector,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// ParquetMediaRow matches Parquet multi-modal export/ingest row layouts.
type ParquetMediaRow struct {
	ID        string    `parquet:"id"`
	Modality  string    `parquet:"modality"`
	MimeType  string    `parquet:"mime_type,optional"`
	SourceURI string    `parquet:"source_uri,optional"`
	Caption   string    `parquet:"caption,optional"`
	Blob      []byte    `parquet:"blob,optional"`
	Vector    []float32 `parquet:"vector,list,optional"`
}

// MultiModalConfig configures CLIP/OpenCLIP and Whisper/CLAP inference endpoints or local backends.
type MultiModalConfig struct {
	Dimension          int    `json:"dimension"`
	ImageModel         string `json:"image_model"`          // e.g. "openclip-vit-b32"
	AudioModel         string `json:"audio_model"`          // e.g. "whisper-clap-v1"
	InferenceEndpoint  string `json:"inference_endpoint"`   // optional Triton / ONNX / OpenCLIP HTTP server
	WASMModelPath      string `json:"wasm_model_path"`      // optional local .wasm/.onnx embedding backend
	InferenceTimeoutMs int    `json:"inference_timeout_ms"` // HTTP inference timeout
}

// MultiModalEmbedder projects text, image, and audio inputs into a shared normalized embedding space.
type MultiModalEmbedder interface {
	Dimension() int
	EmbedText(ctx context.Context, text string) ([]float32, error)
	EmbedImage(ctx context.Context, raw []byte, mimeType, caption string) ([]float32, error)
	EmbedAudio(ctx context.Context, raw []byte, mimeType, transcript string) ([]float32, error)
}

// JointMultiModalEmbedder supports HTTP CLIP/CLAP/Whisper inference, local WASM/ONNX MLBackend,
// and an integrated deterministic pixel+spectral+semantic feature extractor for zero-external-dep execution.
type JointMultiModalEmbedder struct {
	cfg        MultiModalConfig
	httpClient *http.Client
	mlBackend  MLBackend
}

// NewJointMultiModalEmbedder constructs a multi-modal embedder with sensible defaults.
func NewJointMultiModalEmbedder(cfg MultiModalConfig) (*JointMultiModalEmbedder, error) {
	if cfg.Dimension <= 0 {
		cfg.Dimension = 128
	}
	if cfg.ImageModel == "" {
		cfg.ImageModel = "openclip-vit-b32"
	}
	if cfg.AudioModel == "" {
		cfg.AudioModel = "clap-whisper-v1"
	}
	timeout := 10 * time.Second
	if cfg.InferenceTimeoutMs > 0 {
		timeout = time.Duration(cfg.InferenceTimeoutMs) * time.Millisecond
	}
	emb := &JointMultiModalEmbedder{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: timeout},
	}
	if cfg.WASMModelPath != "" {
		backend, err := newMLBackend(cfg.WASMModelPath, cfg.Dimension)
		if err != nil {
			return nil, err
		}
		emb.mlBackend = backend
	}
	return emb, nil
}

// Dimension returns the shared joint embedding dimensionality.
func (e *JointMultiModalEmbedder) Dimension() int {
	return e.cfg.Dimension
}

// EmbedText embeds text into the joint multi-modal embedding space.
func (e *JointMultiModalEmbedder) EmbedText(ctx context.Context, text string) ([]float32, error) {
	if strings.TrimSpace(text) == "" {
		return nil, errors.New("text cannot be empty")
	}
	if e.cfg.InferenceEndpoint != "" {
		if vec, err := e.callInferenceEndpoint(ctx, ModalityText, nil, "text/plain", text); err == nil {
			return vec, nil
		}
	}
	if e.mlBackend != nil {
		if vecs, err := e.mlBackend.Embed(ctx, []string{text}); err == nil && len(vecs) > 0 {
			return normalizeFloat32(vecs[0]), nil
		}
	}
	vec := make([]float32, e.cfg.Dimension)
	projectSemanticTokens(vec, text, 1.0)
	return normalizeFloat32(vec), nil
}

// EmbedImage extracts normalized visual + semantic features for image payloads (CLIP-compatible).
func (e *JointMultiModalEmbedder) EmbedImage(ctx context.Context, raw []byte, mimeType, caption string) ([]float32, error) {
	if len(raw) == 0 && caption == "" {
		return nil, errors.New("image requires non-empty payload or caption")
	}
	if e.cfg.InferenceEndpoint != "" {
		if vec, err := e.callInferenceEndpoint(ctx, ModalityImage, raw, mimeType, caption); err == nil {
			return vec, nil
		}
	}
	vec := make([]float32, e.cfg.Dimension)
	half := e.cfg.Dimension / 2
	if half < 16 {
		half = e.cfg.Dimension
	}
	extractImageSpatialFeatures(vec[:half], raw)
	if caption != "" {
		// Align caption semantics across the whole shared vector so text-to-image queries hit accurately.
		projectSemanticTokens(vec, caption, 1.25)
	}
	return normalizeFloat32(vec), nil
}

// EmbedAudio extracts spectral/temporal + transcript features for WAV/PCM or compressed audio payloads.
func (e *JointMultiModalEmbedder) EmbedAudio(ctx context.Context, raw []byte, mimeType, transcript string) ([]float32, error) {
	if len(raw) == 0 && transcript == "" {
		return nil, errors.New("audio requires non-empty payload or transcript")
	}
	if e.cfg.InferenceEndpoint != "" {
		if vec, err := e.callInferenceEndpoint(ctx, ModalityAudio, raw, mimeType, transcript); err == nil {
			return vec, nil
		}
	}
	vec := make([]float32, e.cfg.Dimension)
	half := e.cfg.Dimension / 2
	if half < 16 {
		half = e.cfg.Dimension
	}
	extractAudioSpectralFeatures(vec[len(vec)-half:], raw)
	if transcript != "" {
		projectSemanticTokens(vec, transcript, 1.25)
	}
	return normalizeFloat32(vec), nil
}

func (e *JointMultiModalEmbedder) callInferenceEndpoint(ctx context.Context, modality MediaModality, raw []byte, mimeType, text string) ([]float32, error) {
	parsed, err := url.Parse(e.cfg.InferenceEndpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("invalid inference endpoint: %q", e.cfg.InferenceEndpoint)
	}
	model := e.cfg.ImageModel
	if modality == ModalityAudio {
		model = e.cfg.AudioModel
	}
	reqBody, err := json.Marshal(map[string]any{
		"model":       model,
		"modality":    string(modality),
		"mime_type":   mimeType,
		"text":        text,
		"data_base64": base64.StdEncoding.EncodeToString(raw),
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.InferenceEndpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.httpClient.Do(req) // #nosec G107 -- endpoint scheme validated above
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("inference server HTTP %d", resp.StatusCode)
	}
	var out struct {
		Embedding []float32   `json:"embedding"`
		Vectors   [][]float32 `json:"vectors"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Embedding) > 0 {
		return normalizeFloat32(out.Embedding), nil
	}
	if len(out.Vectors) > 0 && len(out.Vectors[0]) > 0 {
		return normalizeFloat32(out.Vectors[0]), nil
	}
	return nil, errors.New("inference response missing embedding vector")
}

func projectSemanticTokens(dst []float32, text string, weight float32) {
	tokens := strings.Fields(strings.ToLower(text))
	if len(tokens) == 0 || len(dst) == 0 {
		return
	}
	for _, tok := range tokens {
		tok = strings.Trim(tok, ".,!?;:\"'()[]{}")
		if tok == "" {
			continue
		}
		h := sha256.Sum256([]byte("mm-tok:" + tok))
		for k := 0; k < 4; k++ {
			idx := int(binary.BigEndian.Uint16(h[k*2:k*2+2])) % len(dst)
			sign := float32(1.0)
			if h[16+k]&1 == 1 {
				sign = -1.0
			}
			dst[idx] += sign * weight
		}
	}
}

func extractImageSpatialFeatures(dst []float32, raw []byte) {
	if len(dst) == 0 || len(raw) == 0 {
		return
	}
	img, _, err := image.Decode(bytes.NewReader(raw))
	if err == nil {
		bounds := img.Bounds()
		w, h := bounds.Dx(), bounds.Dy()
		if w > 0 && h > 0 {
			stepX := max(1, w/8)
			stepY := max(1, h/8)
			for y := bounds.Min.Y; y < bounds.Max.Y; y += stepY {
				for x := bounds.Min.X; x < bounds.Max.X; x += stepX {
					r, g, b, _ := img.At(x, y).RGBA()
					gridIdx := ((y-bounds.Min.Y)*4/max(1, h))*4 + ((x - bounds.Min.X) * 4 / max(1, w))
					base := (gridIdx * 3) % len(dst)
					dst[base%len(dst)] += float32(r) / 65535.0
					dst[(base+1)%len(dst)] += float32(g) / 65535.0
					dst[(base+2)%len(dst)] += float32(b) / 65535.0
				}
			}
			return
		}
	}
	// Fallback byte histogram projection for compressed/raw buffers
	for i, b := range raw {
		dst[(int(b)+i)%len(dst)] += 1.0 / float32(len(raw))
	}
}

func extractAudioSpectralFeatures(dst []float32, raw []byte) {
	if len(dst) == 0 || len(raw) == 0 {
		return
	}
	samples := decodeWAVPCM16(raw)
	if len(samples) == 0 {
		for i, b := range raw {
			samples = append(samples, (float32(b)-128.0)/128.0)
			if i >= 2047 {
				break
			}
		}
	}
	if len(samples) > 1024 {
		samples = samples[:1024]
	}
	bands := min(len(dst), 16)
	n := len(samples)
	for b := 0; b < bands; b++ {
		freq := float64(b + 1)
		var realSum, imagSum float64
		for i, s := range samples {
			angle := 2.0 * math.Pi * freq * float64(i) / float64(max(1, n))
			realSum += float64(s) * math.Cos(angle)
			imagSum -= float64(s) * math.Sin(angle)
		}
		mag := math.Sqrt(realSum*realSum+imagSum*imagSum) / float64(max(1, n))
		dst[b] += float32(mag)
	}
	// Zero-crossing & RMS energy
	if len(dst) > bands {
		var sqSum float64
		var zcr int
		for i, s := range samples {
			sqSum += float64(s) * float64(s)
			if i > 0 && ((samples[i-1] >= 0 && s < 0) || (samples[i-1] < 0 && s >= 0)) {
				zcr++
			}
		}
		dst[bands] += float32(math.Sqrt(sqSum / float64(max(1, n))))
		if len(dst) > bands+1 {
			dst[bands+1] += float32(zcr) / float32(max(1, n))
		}
	}
}

func decodeWAVPCM16(raw []byte) []float32 {
	if len(raw) < 44 || string(raw[:4]) != "RIFF" || string(raw[8:12]) != "WAVE" {
		return nil
	}
	dataOffset := 44
	for pos := 12; pos+8 <= len(raw); {
		chunkID := string(raw[pos : pos+4])
		chunkLen := int(binary.LittleEndian.Uint32(raw[pos+4 : pos+8]))
		if chunkLen < 0 {
			break
		}
		if chunkID == "data" {
			dataOffset = pos + 8
			break
		}
		next := pos + 8 + chunkLen
		if chunkLen%2 == 1 {
			next++
		}
		if next <= pos {
			break
		}
		pos = next
	}
	if dataOffset >= len(raw) {
		return nil
	}
	pcm := raw[dataOffset:]
	out := make([]float32, 0, len(pcm)/2)
	for i := 0; i+1 < len(pcm); i += 2 {
		val := int16(binary.LittleEndian.Uint16(pcm[i : i+2])) // #nosec G115 -- valid PCM16 decode
		out = append(out, float32(val)/32768.0)
	}
	return out
}

func normalizeFloat32(v []float32) []float32 {
	out := make([]float32, len(v))
	var sumSq float64
	for _, x := range v {
		sumSq += float64(x) * float64(x)
	}
	if sumSq == 0 {
		copy(out, v)
		return out
	}
	inv := float32(1.0 / math.Sqrt(sumSq))
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}

// MultiModalSearchRequest defines text or media-based cross-modal search options.
type MultiModalSearchRequest struct {
	Dataset     string        `json:"dataset"`
	Modality    MediaModality `json:"modality,omitempty"` // optional filter: "image", "audio", "text"
	TextQuery   string        `json:"text_query,omitempty"`
	ImageBase64 string        `json:"image_base64,omitempty"`
	AudioBase64 string        `json:"audio_base64,omitempty"`
	MediaURI    string        `json:"media_uri,omitempty"`
	MimeType    string        `json:"mime_type,omitempty"`
	K           int           `json:"k"`
}

// MultiModalPipeline manages embedding generation, S3/Parquet ingestion, and cross-modal search.
type MultiModalPipeline struct {
	mu       sync.RWMutex
	store    *VectorStore
	embedder MultiModalEmbedder
}

// NewMultiModalPipeline creates a multi-modal pipeline attached to VectorStore.
func NewMultiModalPipeline(s *VectorStore, cfg MultiModalConfig) (*MultiModalPipeline, error) {
	emb, err := NewJointMultiModalEmbedder(cfg)
	if err != nil {
		return nil, err
	}
	return &MultiModalPipeline{
		store:    s,
		embedder: emb,
	}, nil
}

// SetEmbedder injects a custom/ONNX/mock MultiModalEmbedder.
func (p *MultiModalPipeline) SetEmbedder(emb MultiModalEmbedder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if emb != nil {
		p.embedder = emb
	}
}

// IngestItems embeds missing vectors and stores multi-modal Arrow batches (`blob + modality + vector`).
func (p *MultiModalPipeline) IngestItems(ctx context.Context, dataset string, items []MultiModalItem) (int, error) {
	if dataset == "" {
		return 0, errors.New("dataset is required")
	}
	if len(items) == 0 {
		return 0, nil
	}
	p.mu.RLock()
	emb := p.embedder
	p.mu.RUnlock()

	for i := range items {
		if len(items[i].Blob) == 0 && items[i].BlobB64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(items[i].BlobB64)
			if err != nil {
				return 0, fmt.Errorf("invalid base64 blob for id %q: %w", items[i].ID, err)
			}
			items[i].Blob = decoded
		}
		if len(items[i].Blob) == 0 && items[i].SourceURI != "" {
			raw, err := readMediaURI(ctx, items[i].SourceURI)
			if err == nil {
				items[i].Blob = raw
			}
		}
		if len(items[i].Vector) == 0 {
			vec, err := p.embedItem(ctx, emb, items[i])
			if err != nil {
				return 0, err
			}
			items[i].Vector = vec
		}
		if items[i].Metadata == nil {
			items[i].Metadata = make(map[string]any)
		}
		items[i].Metadata["modality"] = string(items[i].Modality)
		if items[i].MimeType != "" {
			items[i].Metadata["mime_type"] = items[i].MimeType
		}
		if items[i].Caption != "" {
			items[i].Metadata["caption"] = items[i].Caption
		}
		if items[i].SourceURI != "" {
			items[i].Metadata["source_uri"] = items[i].SourceURI
		}
	}

	alloc := memory.DefaultAllocator
	if p.store != nil && p.store.mem != nil {
		alloc = p.store.mem
	}
	rec, err := buildMultiModalArrowBatch(alloc, items, emb.Dimension())
	if err != nil {
		return 0, err
	}
	defer rec.Release()

	if p.store != nil {
		if err := p.store.StoreRecordBatch(ctx, dataset, rec); err != nil {
			return 0, err
		}
	}
	return len(items), nil
}

// IngestParquetFile reads Parquet multi-modal media rows and ingests them via IngestItems.
func (p *MultiModalPipeline) IngestParquetFile(ctx context.Context, dataset, parquetPath string) (int, error) {
	if parquetPath == "" {
		return 0, errors.New("parquet path is required")
	}
	f, err := os.Open(filepath.Clean(parquetPath)) // #nosec G304 -- controlled path
	if err != nil {
		return 0, fmt.Errorf("open parquet media file: %w", err)
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return 0, fmt.Errorf("parse parquet file: %w", err)
	}
	reader := parquet.NewGenericReader[ParquetMediaRow](pf)
	defer func() { _ = reader.Close() }()

	buf := make([]ParquetMediaRow, 128)
	total := 0
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			items := make([]MultiModalItem, 0, n)
			for _, row := range buf[:n] {
				mod := MediaModality(row.Modality)
				if mod == "" {
					mod = ModalityImage
				}
				items = append(items, MultiModalItem{
					ID:        row.ID,
					Modality:  mod,
					MimeType:  row.MimeType,
					SourceURI: row.SourceURI,
					Caption:   row.Caption,
					Blob:      row.Blob,
					Vector:    row.Vector,
				})
			}
			ingested, err := p.IngestItems(ctx, dataset, items)
			if err != nil {
				return total, err
			}
			total += ingested
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return total, readErr
		}
	}
	return total, nil
}

// Search performs cross-modal text-to-image, image-to-text, or audio-to-text search.
func (p *MultiModalPipeline) Search(ctx context.Context, req MultiModalSearchRequest) ([]SearchResult, error) {
	if req.Dataset == "" {
		return nil, errors.New("dataset is required")
	}
	if req.K <= 0 {
		req.K = 10
	}
	p.mu.RLock()
	emb := p.embedder
	p.mu.RUnlock()

	var qVec []float32
	var err error
	switch {
	case req.ImageBase64 != "":
		raw, decErr := base64.StdEncoding.DecodeString(req.ImageBase64)
		if decErr != nil {
			return nil, fmt.Errorf("invalid image_base64: %w", decErr)
		}
		qVec, err = emb.EmbedImage(ctx, raw, req.MimeType, req.TextQuery)
	case req.AudioBase64 != "":
		raw, decErr := base64.StdEncoding.DecodeString(req.AudioBase64)
		if decErr != nil {
			return nil, fmt.Errorf("invalid audio_base64: %w", decErr)
		}
		qVec, err = emb.EmbedAudio(ctx, raw, req.MimeType, req.TextQuery)
	case req.MediaURI != "":
		raw, readErr := readMediaURI(ctx, req.MediaURI)
		if readErr != nil {
			return nil, readErr
		}
		if req.Modality == ModalityAudio {
			qVec, err = emb.EmbedAudio(ctx, raw, req.MimeType, req.TextQuery)
		} else {
			qVec, err = emb.EmbedImage(ctx, raw, req.MimeType, req.TextQuery)
		}
	case req.TextQuery != "":
		qVec, err = emb.EmbedText(ctx, req.TextQuery)
	default:
		return nil, errors.New("provide text_query, image_base64, audio_base64, or media_uri")
	}
	if err != nil {
		return nil, err
	}

	var filters []core.Filter
	if req.Modality != "" {
		filters = append(filters, core.Filter{Field: "modality", Operator: "=", Value: string(req.Modality)})
	}
	return p.store.SearchDatasetVectors(ctx, req.Dataset, qVec, req.K, filters)
}

func (p *MultiModalPipeline) embedItem(ctx context.Context, emb MultiModalEmbedder, item MultiModalItem) ([]float32, error) {
	switch item.Modality {
	case ModalityImage, "":
		return emb.EmbedImage(ctx, item.Blob, item.MimeType, item.Caption)
	case ModalityAudio:
		return emb.EmbedAudio(ctx, item.Blob, item.MimeType, item.Caption)
	case ModalityText:
		return emb.EmbedText(ctx, item.Caption)
	default:
		return nil, fmt.Errorf("unsupported modality: %q", item.Modality)
	}
}

func buildMultiModalArrowBatch(alloc memory.Allocator, items []MultiModalItem, defaultDim int) (arrow.RecordBatch, error) {
	dim := defaultDim
	if len(items) > 0 && len(items[0].Vector) > 0 {
		dim = len(items[0].Vector)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "modality", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "mime_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "source_uri", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "caption", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "blob", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32), Nullable: false}, // #nosec G115
	}, nil)

	idB := array.NewStringBuilder(alloc)
	defer idB.Release()
	modB := array.NewStringBuilder(alloc)
	defer modB.Release()
	mimeB := array.NewStringBuilder(alloc)
	defer mimeB.Release()
	uriB := array.NewStringBuilder(alloc)
	defer uriB.Release()
	capB := array.NewStringBuilder(alloc)
	defer capB.Release()
	blobB := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	defer blobB.Release()
	metaB := array.NewStringBuilder(alloc)
	defer metaB.Release()
	vecListB := array.NewFixedSizeListBuilder(alloc, int32(dim), arrow.PrimitiveTypes.Float32) // #nosec G115
	defer vecListB.Release()
	vecValB := vecListB.ValueBuilder().(*array.Float32Builder)

	for _, it := range items {
		if len(it.Vector) != dim {
			return nil, fmt.Errorf("vector dim mismatch for item %q: got %d expected %d", it.ID, len(it.Vector), dim)
		}
		idB.Append(it.ID)
		mod := string(it.Modality)
		if mod == "" {
			mod = string(ModalityImage)
		}
		modB.Append(mod)
		mimeB.Append(it.MimeType)
		uriB.Append(it.SourceURI)
		capB.Append(it.Caption)
		blobB.Append(it.Blob)
		metaJSON, _ := json.Marshal(it.Metadata)
		metaB.Append(string(metaJSON))
		vecListB.Append(true)
		for _, f := range it.Vector {
			vecValB.Append(f)
		}
	}

	cols := []arrow.Array{
		idB.NewArray(),
		modB.NewArray(),
		mimeB.NewArray(),
		uriB.NewArray(),
		capB.NewArray(),
		blobB.NewArray(),
		metaB.NewArray(),
		vecListB.NewArray(),
	}
	for _, c := range cols {
		defer c.Release()
	}
	return array.NewRecordBatch(schema, cols, int64(len(items))), nil
}

func readMediaURI(ctx context.Context, rawURI string) ([]byte, error) {
	if rawURI == "" {
		return nil, errors.New("empty media URI")
	}
	if strings.HasPrefix(rawURI, "s3://") || strings.HasPrefix(rawURI, "gcs://") || strings.HasPrefix(rawURI, "gs://") {
		backend, err := NewSnapshotBackendForURI(ctx, rawURI)
		if err != nil {
			return nil, err
		}
		parsed, err := url.Parse(rawURI)
		if err != nil {
			return nil, err
		}
		key := strings.TrimPrefix(parsed.Path, "/")
		rc, err := backend.ReadSnapshot(ctx, key)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rc.Close() }()
		return io.ReadAll(io.LimitReader(rc, 64<<20))
	}
	if strings.HasPrefix(rawURI, "file://") {
		parsed, err := url.Parse(rawURI)
		if err != nil {
			return nil, err
		}
		rawURI = parsed.Path
	}
	if strings.HasPrefix(rawURI, "http://") || strings.HasPrefix(rawURI, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURI, nil)
		if err != nil {
			return nil, err
		}
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req) // #nosec G107 -- explicit http/https check
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("media HTTP fetch status %d", resp.StatusCode)
		}
		return io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	}
	return os.ReadFile(filepath.Clean(rawURI)) // #nosec G304 -- caller-configured local path
}

// SearchDatasetVectors provides a shared dataset vector search path with RLS filtering.
func (s *VectorStore) SearchDatasetVectors(ctx context.Context, dataset string, queryVec []float32, k int, filters []core.Filter) ([]SearchResult, error) {
	ds, ok := s.getDataset(dataset)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "dataset not found: %s", dataset)
	}
	ds.dataMu.RLock()
	if ds.Index == nil {
		ds.dataMu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, "dataset has no HNSW index")
	}
	results, err := ds.Index.SearchVectors(ctx, queryVec, k, filters, types.SearchOptions{})
	if err == nil {
		results = s.mapInternalToUserIDsLocked(ds, results)
	}
	ds.dataMu.RUnlock()
	if err != nil {
		return nil, err
	}
	return s.FilterResultsByRLS(ctx, dataset, results), nil
}

func (s *VectorStore) handleMultiModalAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "MultiModalIngest":
		var req struct {
			Dataset string           `json:"dataset"`
			Items   []MultiModalItem `json:"items"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		n, err := s.GetMultiModalPipeline().IngestItems(stream.Context(), req.Dataset, req.Items)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"ingested": n, "dataset": req.Dataset})
		return true, stream.Send(&flight.Result{Body: body})

	case "MultiModalIngestParquet":
		var req struct {
			Dataset     string `json:"dataset"`
			ParquetPath string `json:"parquet_path"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		n, err := s.GetMultiModalPipeline().IngestParquetFile(stream.Context(), req.Dataset, req.ParquetPath)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"ingested": n, "dataset": req.Dataset})
		return true, stream.Send(&flight.Result{Body: body})

	case "MultiModalSearch":
		var req MultiModalSearchRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		res, err := s.GetMultiModalPipeline().Search(stream.Context(), req)
		if err != nil {
			return true, status.Errorf(codes.Internal, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"results": res})
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}
