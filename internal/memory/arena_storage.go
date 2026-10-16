package memory

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Save serializes the arena's contents to the given writer.
func (a *SlabArena) Save(w io.Writer) error {
	slabsPtr := a.slabs.Load()
	if slabsPtr == nil {
		return nil
	}
	slabs := *slabsPtr

	// Write slab capacity
	if err := binary.Write(w, binary.LittleEndian, a.slabCap); err != nil {
		return err
	}
	// Write num slabs
	if err := binary.Write(w, binary.LittleEndian, uint32(len(slabs))); err != nil { // #nosec G115 -- intentional conversion for binary write
		return err
	}

	for _, s := range slabs {
		// Write slab ID and offset
		if err := binary.Write(w, binary.LittleEndian, s.id); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, s.offset); err != nil {
			return err
		}

		// Align to page boundary for mmap
		if seeker, ok := w.(io.Seeker); ok {
			pageSize := os.Getpagesize()
			curr, _ := seeker.Seek(0, io.SeekCurrent)
			padding := (pageSize - (int(curr) % pageSize)) % pageSize
			if padding > 0 {
				if _, err := w.Write(make([]byte, padding)); err != nil {
					return err
				}
			}
		}

		// Write data
		data := s.data
		if len(data) == 0 {
			data = make([]byte, a.slabCap)
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// Load deserializes the arena's contents from the given reader.
func (a *SlabArena) Load(r io.Reader) error {
	var slabCap uint32
	if err := binary.Read(r, binary.LittleEndian, &slabCap); err != nil {
		return err
	}
	a.slabCap = slabCap

	var numSlabs uint32
	if err := binary.Read(r, binary.LittleEndian, &numSlabs); err != nil {
		return err
	}

	slabs := make([]*slab, numSlabs)
	for i := 0; i < int(numSlabs); i++ {
		var id, offset uint32
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &offset); err != nil {
			return err
		}

		// Align to page boundary
		if seeker, ok := r.(io.Seeker); ok {
			pageSize := os.Getpagesize()
			curr, _ := seeker.Seek(0, io.SeekCurrent)
			padding := (pageSize - (int(curr) % pageSize)) % pageSize
			if padding > 0 {
				if _, err := seeker.Seek(int64(padding), io.SeekCurrent); err != nil {
					return err
				}
			}
		}

		data := make([]byte, a.slabCap)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		slabs[i] = &slab{
			id:     id,
			data:   data,
			offset: offset,
		}
	}
	a.slabs.Store(&slabs)
	return nil
}

// LoadMmap maps the arena's contents from the given file using mmap.
func (a *SlabArena) LoadMmap(f *os.File) error {
	var slabCap uint32
	if err := binary.Read(f, binary.LittleEndian, &slabCap); err != nil {
		return err
	}
	a.slabCap = slabCap

	var numSlabs uint32
	if err := binary.Read(f, binary.LittleEndian, &numSlabs); err != nil {
		return err
	}

	slabs := make([]*slab, numSlabs)
	for i := 0; i < int(numSlabs); i++ {
		var id, offset uint32
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			return err
		}
		if err := binary.Read(f, binary.LittleEndian, &offset); err != nil {
			return err
		}

		// Calculate current file offset and align
		currOff, _ := f.Seek(0, io.SeekCurrent)
		pageSize := os.Getpagesize()
		padding := (pageSize - (int(currOff) % pageSize)) % pageSize
		if padding > 0 {
			currOff, _ = f.Seek(int64(padding), io.SeekCurrent)
		}

		// Mmap the slab data
		data, err := Mmap(int(f.Fd()), currOff, int(a.slabCap), true)
		if err != nil {
			return fmt.Errorf("mmap slab %d failed: %v", i, err)
		}

		// Advance file pointer
		if _, err := f.Seek(int64(a.slabCap), io.SeekCurrent); err != nil {
			return err
		}

		slabs[i] = &slab{
			id:     id,
			data:   data,
			offset: offset,
		}
	}
	a.slabs.Store(&slabs)
	return nil
}
