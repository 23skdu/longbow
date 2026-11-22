package query

import (
	"testing"
)

// =============================================================================
// Zero Alloc Parser Extension Tests - Coverage for unexported helpers
// =============================================================================

// --- SkipValue tests (0% coverage) ---

// TestSkipValue_String runs the test skip value_string operation.
func TestSkipValue_String(t *testing.T) {
	data := []byte(`"hello world" remaining`)
	pos, err := SkipValue(data, 0)
	if err != nil {
		t.Errorf("SkipValue failed on string: %v", err)
	}
	if pos < 13 {
		t.Errorf("Position %d should be past string", pos)
	}
}

// TestSkipValue_Number runs the test skip value_number operation.
func TestSkipValue_Number(t *testing.T) {
	data := []byte(`12345.67 remaining`)
	pos, err := SkipValue(data, 0)
	if err != nil {
		t.Errorf("SkipValue failed on number: %v", err)
	}
	if pos == 0 {
		t.Error("Position should advance past number")
	}
}

// TestSkipValue_Object runs the test skip value_object operation.
func TestSkipValue_Object(t *testing.T) {
	data := []byte(`{"key": "value"} remaining`)
	pos, err := SkipValue(data, 0)
	if err != nil {
		t.Errorf("SkipValue failed on object: %v", err)
	}
	_ = pos
}

// TestSkipValue_Array runs the test skip value_array operation.
func TestSkipValue_Array(t *testing.T) {
	data := []byte(`[1, 2, 3] remaining`)
	pos, err := SkipValue(data, 0)
	if err != nil {
		t.Errorf("SkipValue failed on array: %v", err)
	}
	_ = pos
}

// TestSkipValue_True runs the test skip value_true operation.
func TestSkipValue_True(t *testing.T) {
	data := []byte(`true remaining`)
	pos, err := SkipValue(data, 0)
	if err != nil {
		t.Errorf("SkipValue failed on true: %v", err)
	}
	_ = pos
}

// TestSkipValue_False runs the test skip value_false operation.
func TestSkipValue_False(t *testing.T) {
	data := []byte(`false remaining`)
	pos, err := SkipValue(data, 0)
	if err != nil {
		t.Errorf("SkipValue failed on false: %v", err)
	}
	_ = pos
}

// TestSkipValue_Null runs the test skip value_null operation.
func TestSkipValue_Null(t *testing.T) {
	data := []byte(`null remaining`)
	pos, err := SkipValue(data, 0)
	if err != nil {
		t.Errorf("SkipValue failed on null: %v", err)
	}
	_ = pos
}

// --- SkipObject tests (0% coverage) ---

// TestSkipObject_Empty runs the test skip object_empty operation.
func TestSkipObject_Empty(t *testing.T) {
	data := []byte(`{} rest`)
	pos, err := SkipObject(data, 0)
	if err != nil {
		t.Errorf("SkipObject failed on empty: %v", err)
	}
	_ = pos
}

// TestSkipObject_Nested runs the test skip object_nested operation.
func TestSkipObject_Nested(t *testing.T) {
	data := []byte(`{"a": {"b": {"c": 1}}} rest`)
	pos, err := SkipObject(data, 0)
	if err != nil {
		t.Errorf("SkipObject failed on nested: %v", err)
	}
	_ = pos
}

// TestSkipObject_WithArray runs the test skip object_with array operation.
func TestSkipObject_WithArray(t *testing.T) {
	data := []byte(`{"arr": [1, 2, 3], "val": "test"} rest`)
	pos, err := SkipObject(data, 0)
	if err != nil {
		t.Errorf("SkipObject failed with array: %v", err)
	}
	_ = pos
}

// --- SkipArray tests (0% coverage) ---

// TestSkipArray_Empty runs the test skip array_empty operation.
func TestSkipArray_Empty(t *testing.T) {
	data := []byte(`[] rest`)
	pos, err := SkipArray(data, 0)
	if err != nil {
		t.Errorf("SkipArray failed on empty: %v", err)
	}
	_ = pos
}

// TestSkipArray_Nested runs the test skip array_nested operation.
func TestSkipArray_Nested(t *testing.T) {
	data := []byte(`[[1, 2], [3, 4], [[5]]] rest`)
	pos, err := SkipArray(data, 0)
	if err != nil {
		t.Errorf("SkipArray failed on nested: %v", err)
	}
	_ = pos
}

// TestSkipArray_MixedTypes runs the test skip array_mixed types operation.
func TestSkipArray_MixedTypes(t *testing.T) {
	data := []byte(`[1, "str", true, null, {"key": "val"}] rest`)
	pos, err := SkipArray(data, 0)
	if err != nil {
		t.Errorf("SkipArray failed on mixed: %v", err)
	}
	_ = pos
}

// --- SkipLiteral tests (0% coverage) ---

// TestSkipLiteral_True runs the test skip literal_true operation.
func TestSkipLiteral_True(t *testing.T) {
	data := []byte(`true, next`)
	pos, err := SkipLiteral(data, 0)
	if err != nil {
		t.Errorf("SkipLiteral failed on true: %v", err)
	}
	if pos != 4 {
		t.Errorf("Expected position 4, got %d", pos)
	}
}

// TestSkipLiteral_False runs the test skip literal_false operation.
func TestSkipLiteral_False(t *testing.T) {
	data := []byte(`false, next`)
	pos, err := SkipLiteral(data, 0)
	if err != nil {
		t.Errorf("SkipLiteral failed on false: %v", err)
	}
	if pos != 5 {
		t.Errorf("Expected position 5, got %d", pos)
	}
}

// TestSkipLiteral_Null runs the test skip literal_null operation.
func TestSkipLiteral_Null(t *testing.T) {
	data := []byte(`null, next`)
	pos, err := SkipLiteral(data, 0)
	if err != nil {
		t.Errorf("SkipLiteral failed on null: %v", err)
	}
	if pos != 4 {
		t.Errorf("Expected position 4, got %d", pos)
	}
}

// --- SkipNumber tests (0% coverage) ---

// TestSkipNumber_Integer runs the test skip number_integer operation.
func TestSkipNumber_Integer(t *testing.T) {
	data := []byte(`12345, next`)
	pos, err := SkipNumber(data, 0)
	if err != nil {
		t.Errorf("SkipNumber failed on integer: %v", err)
	}
	_ = pos
}

// TestSkipNumber_Negative runs the test skip number_negative operation.
func TestSkipNumber_Negative(t *testing.T) {
	data := []byte(`-9876, next`)
	pos, err := SkipNumber(data, 0)
	if err != nil {
		t.Errorf("SkipNumber failed on negative: %v", err)
	}
	_ = pos
}

// TestSkipNumber_Float runs the test skip number_float operation.
func TestSkipNumber_Float(t *testing.T) {
	data := []byte(`123.456, next`)
	pos, err := SkipNumber(data, 0)
	if err != nil {
		t.Errorf("SkipNumber failed on float: %v", err)
	}
	_ = pos
}

// TestSkipNumber_Scientific runs the test skip number_scientific operation.
func TestSkipNumber_Scientific(t *testing.T) {
	data := []byte(`1.5e-10, next`)
	pos, err := SkipNumber(data, 0)
	if err != nil {
		t.Errorf("SkipNumber failed on scientific: %v", err)
	}
	_ = pos
}

// TestSkipNumber_ScientificUpperE runs the test skip number_scientific upper e operation.
func TestSkipNumber_ScientificUpperE(t *testing.T) {
	data := []byte(`2.5E+20, next`)
	pos, err := SkipNumber(data, 0)
	if err != nil {
		t.Errorf("SkipNumber failed on scientific E: %v", err)
	}
	_ = pos
}

// --- encodeRune tests (17.6% coverage) ---

// TestEncodeRune_ASCII runs the test encode rune_ascii operation.
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

// TestEncodeRune_TwoByte runs the test encode rune_two byte operation.
func TestEncodeRune_TwoByte(t *testing.T) {
	buf := make([]byte, 4)
	// U+00E9 = é (2-byte UTF-8)
	n := encodeRune(buf, 0x00E9)
	if n != 2 {
		t.Errorf("Expected 2 bytes for 2-byte char, got %d", n)
	}
}

// TestEncodeRune_ThreeByte runs the test encode rune_three byte operation.
func TestEncodeRune_ThreeByte(t *testing.T) {
	buf := make([]byte, 4)
	// U+4E2D = 中 (3-byte UTF-8)
	n := encodeRune(buf, 0x4E2D)
	if n != 3 {
		t.Errorf("Expected 3 bytes for CJK char, got %d", n)
	}
}

// TestEncodeRune_FourByte runs the test encode rune_four byte operation.
func TestEncodeRune_FourByte(t *testing.T) {
	buf := make([]byte, 4)
	// U+1F600 = 😀 (4-byte UTF-8)
	n := encodeRune(buf, 0x1F600)
	if n != 4 {
		t.Errorf("Expected 4 bytes for emoji, got %d", n)
	}
}

// TestEncodeRune_MaxRune runs the test encode rune_max rune operation.
func TestEncodeRune_MaxRune(t *testing.T) {
	buf := make([]byte, 4)
	// Max valid Unicode code point
	n := encodeRune(buf, 0x10FFFF)
	if n != 4 {
		t.Errorf("Expected 4 bytes for max rune, got %d", n)
	}
}

// TestEncodeRune_Invalid runs the test encode rune_invalid operation.
func TestEncodeRune_Invalid(t *testing.T) {
	buf := make([]byte, 4)
	// Invalid rune beyond Unicode range - should handle gracefully
	n := encodeRune(buf, 0x110000)
	_ = n // Just verify no panic
}

// --- ParseString edge cases ---

// TestParseString_Simple runs the test parse string_simple operation.
func TestParseString_Simple(t *testing.T) {
	data := []byte(`"hello" rest`)
	s, pos, err := ParseString(data, 0)
	if err != nil {
		t.Errorf("ParseString failed: %v", err)
	}
	if s != "hello" {
		t.Errorf("Expected hello, got %s", s)
	}
	_ = pos
}

// TestParseString_WithEscape runs the test parse string_with escape operation.
func TestParseString_WithEscape(t *testing.T) {
	data := []byte(`"hello\nworld" rest`)
	s, pos, err := ParseString(data, 0)
	if err != nil {
		t.Errorf("ParseString failed: %v", err)
	}
	if s != "hello\nworld" {
		t.Errorf("Expected hello\\nworld, got %s", s)
	}
	_ = pos
}

// TestParseString_WithUnicode runs the test parse string_with unicode operation.
func TestParseString_WithUnicode(t *testing.T) {
	// json.Marshal encodes > as \u003e
	data := []byte(`"a\u003eb" rest`)
	s, pos, err := ParseString(data, 0)
	if err != nil {
		t.Errorf("ParseString failed: %v", err)
	}
	if s != "a>b" {
		t.Errorf("Expected a>b, got %s", s)
	}
	_ = pos
}

// --- decodeEscapes edge cases ---

// TestDecodeEscapes_Tab runs the test decode escapes_tab operation.
func TestDecodeEscapes_Tab(t *testing.T) {
	data := []byte(`hello\tworld`)
	result := decodeEscapes(data)
	// decodeEscapes converts \t to actual tab
	_ = result // Just verify no panic
}

// TestDecodeEscapes_Backslash runs the test decode escapes_backslash operation.
func TestDecodeEscapes_Backslash(t *testing.T) {
	data := []byte(`path\\file`)
	result := decodeEscapes(data)
	_ = result // Just verify no panic
}

// TestDecodeEscapes_Quote runs the test decode escapes_quote operation.
func TestDecodeEscapes_Quote(t *testing.T) {
	data := []byte(`say \"hello\"`)
	result := decodeEscapes(data)
	_ = result // Just verify no panic
}
