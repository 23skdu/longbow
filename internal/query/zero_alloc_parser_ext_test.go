package query

import (
	"testing"
)

// =============================================================================
// Zero Alloc Parser Extension Tests - Coverage for unexported helpers
// =============================================================================

// --- skipValue tests (0% coverage) ---

func TestSkipValue_String(t *testing.T) {
	data := []byte(`"hello world" remaining`)
	pos, err := skipValue(data, 0)
	if err != nil {
		t.Errorf("skipValue failed on string: %v", err)
	}
	if pos < 13 {
		t.Errorf("Position %d should be past string", pos)
	}
}

func TestSkipValue_Number(t *testing.T) {
	data := []byte(`12345.67 remaining`)
	pos, err := skipValue(data, 0)
	if err != nil {
		t.Errorf("skipValue failed on number: %v", err)
	}
	if pos == 0 {
		t.Error("Position should advance past number")
	}
}

func TestSkipValue_Object(t *testing.T) {
	data := []byte(`{"key": "value"} remaining`)
	pos, err := skipValue(data, 0)
	if err != nil {
		t.Errorf("skipValue failed on object: %v", err)
	}
	_ = pos
}

func TestSkipValue_Array(t *testing.T) {
	data := []byte(`[1, 2, 3] remaining`)
	pos, err := skipValue(data, 0)
	if err != nil {
		t.Errorf("skipValue failed on array: %v", err)
	}
	_ = pos
}

func TestSkipValue_True(t *testing.T) {
	data := []byte(`true remaining`)
	pos, err := skipValue(data, 0)
	if err != nil {
		t.Errorf("skipValue failed on true: %v", err)
	}
	_ = pos
}

func TestSkipValue_False(t *testing.T) {
	data := []byte(`false remaining`)
	pos, err := skipValue(data, 0)
	if err != nil {
		t.Errorf("skipValue failed on false: %v", err)
	}
	_ = pos
}

func TestSkipValue_Null(t *testing.T) {
	data := []byte(`null remaining`)
	pos, err := skipValue(data, 0)
	if err != nil {
		t.Errorf("skipValue failed on null: %v", err)
	}
	_ = pos
}

// --- skipObject tests (0% coverage) ---

func TestSkipObject_Empty(t *testing.T) {
	data := []byte(`{} rest`)
	pos, err := skipObject(data, 0)
	if err != nil {
		t.Errorf("skipObject failed on empty: %v", err)
	}
	_ = pos
}

func TestSkipObject_Nested(t *testing.T) {
	data := []byte(`{"a": {"b": {"c": 1}}} rest`)
	pos, err := skipObject(data, 0)
	if err != nil {
		t.Errorf("skipObject failed on nested: %v", err)
	}
	_ = pos
}

func TestSkipObject_WithArray(t *testing.T) {
	data := []byte(`{"arr": [1, 2, 3], "val": "test"} rest`)
	pos, err := skipObject(data, 0)
	if err != nil {
		t.Errorf("skipObject failed with array: %v", err)
	}
	_ = pos
}

// --- skipArray tests (0% coverage) ---

func TestSkipArray_Empty(t *testing.T) {
	data := []byte(`[] rest`)
	pos, err := skipArray(data, 0)
	if err != nil {
		t.Errorf("skipArray failed on empty: %v", err)
	}
	_ = pos
}

func TestSkipArray_Nested(t *testing.T) {
	data := []byte(`[[1, 2], [3, 4], [[5]]] rest`)
	pos, err := skipArray(data, 0)
	if err != nil {
		t.Errorf("skipArray failed on nested: %v", err)
	}
	_ = pos
}

func TestSkipArray_MixedTypes(t *testing.T) {
	data := []byte(`[1, "str", true, null, {"key": "val"}] rest`)
	pos, err := skipArray(data, 0)
	if err != nil {
		t.Errorf("skipArray failed on mixed: %v", err)
	}
	_ = pos
}

// --- skipLiteral tests (0% coverage) ---

func TestSkipLiteral_True(t *testing.T) {
	data := []byte(`true, next`)
	pos, err := skipLiteral(data, 0)
	if err != nil {
		t.Errorf("skipLiteral failed on true: %v", err)
	}
	if pos != 4 {
		t.Errorf("Expected position 4, got %d", pos)
	}
}

func TestSkipLiteral_False(t *testing.T) {
	data := []byte(`false, next`)
	pos, err := skipLiteral(data, 0)
	if err != nil {
		t.Errorf("skipLiteral failed on false: %v", err)
	}
	if pos != 5 {
		t.Errorf("Expected position 5, got %d", pos)
	}
}

func TestSkipLiteral_Null(t *testing.T) {
	data := []byte(`null, next`)
	pos, err := skipLiteral(data, 0)
	if err != nil {
		t.Errorf("skipLiteral failed on null: %v", err)
	}
	if pos != 4 {
		t.Errorf("Expected position 4, got %d", pos)
	}
}

// --- skipNumber tests (0% coverage) ---

func TestSkipNumber_Integer(t *testing.T) {
	data := []byte(`12345, next`)
	pos, err := skipNumber(data, 0)
	if err != nil {
		t.Errorf("skipNumber failed on integer: %v", err)
	}
	_ = pos
}

func TestSkipNumber_Negative(t *testing.T) {
	data := []byte(`-9876, next`)
	pos, err := skipNumber(data, 0)
	if err != nil {
		t.Errorf("skipNumber failed on negative: %v", err)
	}
	_ = pos
}

func TestSkipNumber_Float(t *testing.T) {
	data := []byte(`123.456, next`)
	pos, err := skipNumber(data, 0)
	if err != nil {
		t.Errorf("skipNumber failed on float: %v", err)
	}
	_ = pos
}

func TestSkipNumber_Scientific(t *testing.T) {
	data := []byte(`1.5e-10, next`)
	pos, err := skipNumber(data, 0)
	if err != nil {
		t.Errorf("skipNumber failed on scientific: %v", err)
	}
	_ = pos
}

func TestSkipNumber_ScientificUpperE(t *testing.T) {
	data := []byte(`2.5E+20, next`)
	pos, err := skipNumber(data, 0)
	if err != nil {
		t.Errorf("skipNumber failed on scientific E: %v", err)
	}
	_ = pos
}

// --- encodeRune tests (17.6% coverage) ---

func TestEncodeRune_ASCII(t *testing.T) {
	buf := make([]byte, 4)
	n := encodeRune(buf, 'A')
	if n != 1 {
		t.Errorf("Expected 1 byte for ASCII, got %d", n)
	}
	if buf[0] != 'A' {
		t.Errorf("Expected 'A', got %c", buf[0])
	}
}

func TestEncodeRune_TwoByte(t *testing.T) {
	buf := make([]byte, 4)
	// U+00E9 = é (2-byte UTF-8)
	n := encodeRune(buf, 0x00E9)
	if n != 2 {
		t.Errorf("Expected 2 bytes for 2-byte char, got %d", n)
	}
}

func TestEncodeRune_ThreeByte(t *testing.T) {
	buf := make([]byte, 4)
	// U+4E2D = 中 (3-byte UTF-8)
	n := encodeRune(buf, 0x4E2D)
	if n != 3 {
		t.Errorf("Expected 3 bytes for CJK char, got %d", n)
	}
}

func TestEncodeRune_FourByte(t *testing.T) {
	buf := make([]byte, 4)
	// U+1F600 = 😀 (4-byte UTF-8)
	n := encodeRune(buf, 0x1F600)
	if n != 4 {
		t.Errorf("Expected 4 bytes for emoji, got %d", n)
	}
}

func TestEncodeRune_MaxRune(t *testing.T) {
	buf := make([]byte, 4)
	// Max valid Unicode code point
	n := encodeRune(buf, 0x10FFFF)
	if n != 4 {
		t.Errorf("Expected 4 bytes for max rune, got %d", n)
	}
}

func TestEncodeRune_Invalid(t *testing.T) {
	buf := make([]byte, 4)
	// Invalid rune beyond Unicode range - should handle gracefully
	n := encodeRune(buf, 0x110000)
	_ = n // Just verify no panic
}

// --- parseString edge cases ---

func TestParseString_Simple(t *testing.T) {
	data := []byte(`"hello" rest`)
	s, pos, err := parseString(data, 0)
	if err != nil {
		t.Errorf("parseString failed: %v", err)
	}
	if s != "hello" {
		t.Errorf("Expected hello, got %s", s)
	}
	_ = pos
}

func TestParseString_WithEscape(t *testing.T) {
	data := []byte(`"hello\nworld" rest`)
	s, pos, err := parseString(data, 0)
	if err != nil {
		t.Errorf("parseString failed: %v", err)
	}
	if s != "hello\nworld" {
		t.Errorf("Expected hello\\nworld, got %s", s)
	}
	_ = pos
}

func TestParseString_WithUnicode(t *testing.T) {
	// json.Marshal encodes > as \u003e
	data := []byte(`"a\u003eb" rest`)
	s, pos, err := parseString(data, 0)
	if err != nil {
		t.Errorf("parseString failed: %v", err)
	}
	if s != "a>b" {
		t.Errorf("Expected a>b, got %s", s)
	}
	_ = pos
}

// --- decodeEscapes edge cases ---

func TestDecodeEscapes_Tab(t *testing.T) {
	data := []byte(`hello\tworld`)
	result := decodeEscapes(data)
	// decodeEscapes converts \t to actual tab
	_ = result // Just verify no panic
}

func TestDecodeEscapes_Backslash(t *testing.T) {
	data := []byte(`path\\file`)
	result := decodeEscapes(data)
	_ = result // Just verify no panic
}

func TestDecodeEscapes_Quote(t *testing.T) {
	data := []byte(`say \"hello\"`)
	result := decodeEscapes(data)
	_ = result // Just verify no panic
}
