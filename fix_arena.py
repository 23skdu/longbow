import re

with open('internal/memory/arena.go', 'r') as f:
    content = f.read()

old_code = """	for i := range slabs {
		newData := alloc.Allocate(len(slabs[i].data))
		if newData == nil {
			return fmt.Errorf("off-heap allocation failed")
		}
		copy(newData, slabs[i].data)
		slabs[i].data = newData
	}
	a.alloc = alloc
	return nil"""

new_code = """	newSlabs := make([]*slab, len(slabs))
	for i := range slabs {
		newData := alloc.Allocate(len(slabs[i].data))
		if newData == nil {
			return fmt.Errorf("off-heap allocation failed")
		}
		copy(newData, slabs[i].data)
		newSlabs[i] = &slab{
			id:         slabs[i].id,
			generation: slabs[i].generation,
			data:       newData,
			offset:     slabs[i].offset,
		}
	}
	a.slabs.Store(&newSlabs)
	a.alloc = alloc
	return nil"""

content = content.replace(old_code, new_code)

with open('internal/memory/arena.go', 'w') as f:
    f.write(content)
