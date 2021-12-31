package bytepool

type FrameBuffer struct {
	*StringsBuffer
	offsets []int
}

func NewFrameBuffer() *FrameBuffer {
	return &FrameBuffer{
		StringsBuffer: NewStringsBuffer(),
		offsets:       make([]int, 0),
	}
}

func (f *FrameBuffer) NewRow() {
	f.offsets = append(f.offsets, f.StringsBuffer.Len())
}

// DiscardCurrentRow unreads data after last NextRow is called
func (f *FrameBuffer) DiscardCurrentRow() {
	if len(f.offsets) == 0 {
		return
	}
	lastOffset := f.lastOffset()
	f.StringsBuffer.TruncateElem(lastOffset)
	f.offsets = f.offsets[:len(f.offsets)-1]
}

func (f *FrameBuffer) lastOffset() int {
	return f.offsets[len(f.offsets)-1]
}

// Export attempts to put the result into 2 dimensional string.
// returns the number of rows read and each number of elems
// in each row
func (f *FrameBuffer) Export(result [][]string) (int, []int) {
	if len(result) == 0 || len(f.offsets) == 0 {
		return 0, []int{}
	}

	sbResult := f.StringsBuffer.Export()

	offsets := append(f.offsets[1:], f.StringsBuffer.Len())
	elems_read := make([]int, 0, len(offsets))
	var rows_read int
	var lastOffset int
	for rows_read < len(result) && rows_read < len(offsets) {
		currentOffset := offsets[rows_read]
		result[rows_read] = sbResult[lastOffset:currentOffset]
		elems_read = append(elems_read, currentOffset-lastOffset)
		lastOffset = currentOffset
		rows_read++
	}

	return rows_read, elems_read
}

// ReadColumnTexts attempts to read into ReadColumnTexts
// May skip columns or panic if dimension are not correct
// Caller should check the size of input before calling this function
// return number of rows read and number of elems read in each row
func (f *FrameBuffer) ReadColumnTexts(columnTexts [][]string) (int, []int) {
	if len(columnTexts) == 0 || len(columnTexts[0]) == 0 || len(f.offsets) == 0 {
		return 0, []int{}
	}

	sbResult := f.StringsBuffer.Export()

	offsets := append(f.offsets[1:], f.StringsBuffer.Len())
	elems_read := make([]int, 0, len(offsets))
	var rows_read int
	var lastOffset int
	for rows_read < len(columnTexts[0]) && rows_read < len(offsets) {
		currentOffset := offsets[rows_read]
		row := sbResult[lastOffset:currentOffset]
		for i, col := range columnTexts {
			col[rows_read] = row[i]
		}
		elems_read = append(elems_read, currentOffset-lastOffset)
		lastOffset = currentOffset
		rows_read++
	}

	return rows_read, elems_read
}
