package main

import (
	"math/rand"
	"testing"
)

func TestBuildSchema(t *testing.T) {
	s := buildSchema(4)
	if s == nil || s.NumFields() != 3 {
		t.Fatalf("unexpected schema")
	}
}

func TestRandVec(t *testing.T) {
	v := randVec(rand.New(rand.NewSource(1)), 4)
	if len(v) != 4 {
		t.Fatal("bad vec")
	}
	_ = v
}

func FuzzRandVec(f *testing.F) {
	f.Add(4)
	f.Fuzz(func(t *testing.T, d int) {
		if d < 1 {
			d = 1
		}
		if d > 64 {
			d = 64
		}
		v := randVec(rand.New(rand.NewSource(1)), d)
		if len(v) != d {
			t.Fatal("bad vec")
		}
	})
}
