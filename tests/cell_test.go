package test

import (
	"testing"

	cell "github.com/ish4n10/miniaturedb/storage/cell"
)

func writeRead(t *testing.T, c *cell.Cell) *cell.Cell {
	t.Helper()
	buf := make([]byte, 512)
	_, err := cell.Write(buf, 0, c)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	got, _, err := cell.Read(buf, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	return got
}
func TestCell_Descriptor_LongData(t *testing.T) {
	buf := make([]byte, 512)
	cell.Write(buf, 0, &cell.Cell{Type: cell.CellTypeValue, Data: make([]byte, 300)})
	if buf[0]&0x08 != 0 {
		t.Fatal("short flag should NOT be set for 300 bytes")
	}
}

func TestCell_WriteRead(t *testing.T) {
	cases := []struct {
		typ  cell.CellType
		data string
	}{
		{cell.CellTypeKey, "name"},
		{cell.CellTypeValue, `{"name":"alice"}`},
		{cell.CellTypeDeleted, ""},
	}
	for _, c := range cases {
		got := writeRead(t, &cell.Cell{Type: c.typ, Data: []byte(c.data)})
		if got.Type != c.typ {
			t.Fatalf("wrong type: want %d got %d", c.typ, got.Type)
		}
		if string(got.Data) != c.data {
			t.Fatalf("wrong data: want %q got %q", c.data, got.Data)
		}
	}
}

func TestCell_WriteRead_LongData(t *testing.T) {
	data := make([]byte, 300)
	for i := range data {
		data[i] = byte(i % 256)
	}
	got := writeRead(t, &cell.Cell{Type: cell.CellTypeValue, Data: data})
	if len(got.Data) != 300 {
		t.Fatalf("expected 300 bytes, got %d", len(got.Data))
	}
	for i, b := range got.Data {
		if b != byte(i%256) {
			t.Fatalf("data mismatch at byte %d", i)
		}
	}
}

func TestReadAll(t *testing.T) {
	buf := make([]byte, 256)
	off := 0
	for i := 1; i <= 3; i++ {
		off, _ = cell.Write(buf, off, &cell.Cell{Type: cell.CellTypeKey, Data: []byte("k")})
		off, _ = cell.Write(buf, off, &cell.Cell{Type: cell.CellTypeValue, Data: []byte("v")})
	}

	cells, err := cell.ReadAll(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 6 {
		t.Fatalf("expected 6 cells, got %d", len(cells))
	}
}
