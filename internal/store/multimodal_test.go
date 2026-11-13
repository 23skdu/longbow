package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func makeTestPNG(c color.RGBA) []byte {
	img := image.NewRGBA(image.Rect(0, 0, 16, 16))
	for y := 0; y < 16; y++ {
		for x := 0; x < 16; x++ {
			img.Set(x, y, c)
		}
	}
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return buf.Bytes()
}

func makeTestWAV(freqSamples int) []byte {
	var buf bytes.Buffer
	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(36+freqSamples*2))
	buf.WriteString("WAVEfmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16000))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(32000))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(2))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(16))
	buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(freqSamples*2))
	for i := 0; i < freqSamples; i++ {
		val := int16((i % 32) * 500)
		_ = binary.Write(&buf, binary.LittleEndian, val)
	}
	return buf.Bytes()
}

func TestMultiModalIngestParquetAndCrossModalSearch(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	defer func() { _ = vs.Close() }()

	pipe := vs.GetMultiModalPipeline()
	ctx := context.Background()

	sunsetImg := makeTestPNG(color.RGBA{R: 255, G: 120, B: 40, A: 255})
	oceanImg := makeTestPNG(color.RGBA{R: 20, G: 100, B: 230, A: 255})
	audioClip := makeTestWAV(256)

	n, err := pipe.IngestItems(ctx, "media_ds", []MultiModalItem{
		{ID: "img-sunset", Modality: ModalityImage, MimeType: "image/png", Caption: "sunset over golden beach", Blob: sunsetImg},
		{ID: "img-ocean", Modality: ModalityImage, MimeType: "image/png", Caption: "deep blue ocean waves", Blob: oceanImg},
		{ID: "aud-chime", Modality: ModalityAudio, MimeType: "audio/wav", Caption: "morning piano chime melody", Blob: audioClip},
	})
	require.NoError(t, err)
	require.Equal(t, 3, n)

	// Parquet multi-modal batch ingestion
	pqPath := filepath.Join(t.TempDir(), "media.parquet")
	require.NoError(t, parquet.WriteFile(pqPath, []ParquetMediaRow{
		{ID: "pq-mountain", Modality: "image", MimeType: "image/png", Caption: "snowy alpine mountain peak", Blob: oceanImg},
	}))
	pqCount, err := pipe.IngestParquetFile(ctx, "media_ds", pqPath)
	require.NoError(t, err)
	require.Equal(t, 1, pqCount)

	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset("media_ds")
		return ok && ds.IndexLen() >= 4
	}, 2*time.Second, 20*time.Millisecond)

	// Text-to-image search should return indexed image records
	res, err := pipe.Search(ctx, MultiModalSearchRequest{
		Dataset:   "media_ds",
		Modality:  ModalityImage,
		TextQuery: "sunset golden beach",
		K:         2,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res)

	// Mock external HTTP CLIP inference server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{1, 0, 0, 0},
		})
	}))
	defer srv.Close()

	httpEmb, err := NewJointMultiModalEmbedder(MultiModalConfig{
		Dimension:         4,
		InferenceEndpoint: srv.URL,
	})
	require.NoError(t, err)
	v, err := httpEmb.EmbedImage(ctx, sunsetImg, "image/png", "test")
	require.NoError(t, err)
	require.Len(t, v, 4)
}

func BenchmarkMultiModalPipeline(b *testing.B) {
	emb, _ := NewJointMultiModalEmbedder(MultiModalConfig{Dimension: 128})
	img := makeTestPNG(color.RGBA{R: 200, G: 100, B: 50, A: 255})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := emb.EmbedImage(ctx, img, "image/png", "sunset mountain horizon")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzMultiModalEmbedding(f *testing.F) {
	f.Add([]byte("RIFF\x24\x00\x00\x00WAVEfmt "), "sunset sky")
	f.Add(makeTestPNG(color.RGBA{R: 1, G: 2, B: 3, A: 255}), "blue")

	f.Fuzz(func(t *testing.T, payload []byte, caption string) {
		emb, err := NewJointMultiModalEmbedder(MultiModalConfig{Dimension: 64})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = emb.EmbedImage(context.Background(), payload, "image/png", caption)
		_, _ = emb.EmbedAudio(context.Background(), payload, "audio/wav", caption)
		_, _ = emb.EmbedText(context.Background(), caption)
	})
}
