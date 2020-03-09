// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

// TODO(yuzefovich): support rehashing instead of large fixed bucket size.
const hashTableNumBuckets = 1 << 16

// hashTableMode represents different modes in which the hashTable is built.
type hashTableMode int

const (
	// hashTableFullMode is the mode where hashTable buffers all input tuples and
	// only populates first and next array for each hash bucket.
	hashTableFullMode hashTableMode = iota

	// hashTableDistinctMode is the mode where hashTable only buffers distinct
	// tuples.
	hashTableDistinctMode
)

// hashTableBuildBuffer stores the information related to the build table.
type hashTableBuildBuffer struct {
	// first stores the keyID of the first key that resides in each bucket. This
	// keyID is used to determine the corresponding equality column key as well
	// as output column values.
	first []uint64

	// next is a densely-packed list that stores the keyID of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	next []uint64

	// TODO(azhng): doc
	last []uint64
}

// hashTableProbeBuffer stores the information related to the probe table.
type hashTableProbeBuffer struct {
	// first stores the keyID of the first key that resides in each bucket. This
	// keyID is used to determine the corresponding equality column key as well
	// as output column values.
	first []uint64

	// next is a densely-packed list that stores the keyID of the next key in the
	// hash table bucket chain, where an id of 0 is reserved to represent end of
	// chain.
	next []uint64

	// TODO(azhng): doc
	prev []uint64
	last []uint64

	// headID stores the first build table keyID that matched with the probe batch
	// key at any given index.
	headID []uint64

	// differs stores whether the key at any index differs with the build table
	// key.
	differs []bool

	// distinct stores whether the key in the probe batch is distinct in the build
	// table.
	distinct []bool

	// keys stores the equality columns on the probe table for a single batch.
	keys []coldata.Vec
	// buckets is used to store the computed hash value of each key in a single
	// batch.
	buckets []uint64

	// groupID stores the keyID that maps to the joining rows of the build table.
	// The ith element of groupID stores the keyID of the build table that
	// corresponds to the ith key in the probe table.
	groupID []uint64
	// toCheck stores the indices of the eqCol rows that have yet to be found or
	// rejected.
	toCheck []uint64

	// hashBuffer stores the hash values of each tuple in the probe table. It will
	// be dynamically updated when the hashTable is build in distinct mode.
	hashBuffer []uint64
}

// hashTable is a structure used by the hash joiner to store the build table
// batches. Keys are stored according to the encoding of the equality column,
// which point to the corresponding output keyID. The keyID is calculated
// using the below equation:
//
// keyID = keys.indexOf(key) + 1
//
// and inversely:
//
// keys[keyID - 1] = key
//
// The table can then be probed in column batches to find at most one matching
// row per column batch row.
type hashTable struct {
	allocator *Allocator

	// buildScratch contains the scratch buffers required for the build table.
	buildScratch hashTableBuildBuffer

	// probeScratch contains the scratch buffers required for the probe table.
	probeScratch hashTableProbeBuffer

	// same and visited are only used when the hashTable contains non-distinct
	// keys.
	//
	// same is a densely-packed list that stores the keyID of the next key in the
	// hash table that has the same value as the current key. The headID of the key
	// is the first key of that value found in the next linked list. This field
	// will be lazily populated by the prober.
	same []uint64
	// visited represents whether each of the corresponding keys have been touched
	// by the prober.
	visited []bool

	// vals stores the union of the equality and output columns of the build
	// table. A key tuple is defined as the elements in each row of vals that
	// makes up the equality columns. The ID of a key at any index of vals is
	// index + 1.
	vals coldata.Batch
	// valTypes stores the corresponding types of the val columns.
	valTypes []coltypes.T
	// valCols stores the union of the keyCols and outCols.
	valCols []uint32
	// keyCols stores the corresponding types of key columns.
	keyTypes []coltypes.T
	// keyCols stores the indices of vals which are key columns.
	keyCols []uint32

	// outCols stores the indices of vals which are output columns.
	outCols []uint32
	// outTypes stores the types of the output columns.
	outTypes []coltypes.T

	// numBuckets returns the number of buckets the hashTable employs. This is
	// equivalent to the size of first.
	numBuckets uint64

	// allowNullEquality determines if NULL keys should be treated as equal to
	// each other.
	allowNullEquality bool

	decimalScratch decimalOverloadScratch
	cancelChecker  CancelChecker

	// mode determines how hashTable is built.
	mode hashTableMode
}

var _ resetter = &hashTable{}

func newHashTable(
	allocator *Allocator,
	numBuckets uint64,
	sourceTypes []coltypes.T,
	eqCols []uint32,
	outCols []uint32,
	allowNullEquality bool,
	mode hashTableMode,
) *hashTable {
	// Compute the union of eqCols and outCols and compress vals to only keep the
	// important columns.
	nCols := len(sourceTypes)
	keepCol := make([]bool, nCols)
	compressed := make([]uint32, nCols)
	for _, colIdx := range eqCols {
		keepCol[colIdx] = true
	}

	for _, colIdx := range outCols {
		keepCol[colIdx] = true
	}

	// Extract the important columns and discard the rest.
	nKeep := uint32(0)

	keepTypes := make([]coltypes.T, 0, nCols)
	keepCols := make([]uint32, 0, nCols)

	for i := 0; i < nCols; i++ {
		if keepCol[i] {
			keepTypes = append(keepTypes, sourceTypes[i])
			keepCols = append(keepCols, uint32(i))

			compressed[i] = nKeep
			nKeep++
		}
	}

	// Extract and types and indices of the	eqCols and outCols.
	nKeys := len(eqCols)
	keyTypes := make([]coltypes.T, nKeys)
	keys := make([]uint32, nKeys)

	for i, colIdx := range eqCols {
		keyTypes[i] = sourceTypes[colIdx]
		keys[i] = compressed[colIdx]
	}

	nOutCols := len(outCols)
	outTypes := make([]coltypes.T, nOutCols)
	outs := make([]uint32, nOutCols)
	for i, colIdx := range outCols {
		outTypes[i] = sourceTypes[colIdx]
		outs[i] = compressed[colIdx]
	}

	ht := &hashTable{
		allocator: allocator,

		buildScratch: hashTableBuildBuffer{
			first: make([]uint64, numBuckets),
		},

		probeScratch: hashTableProbeBuffer{
			keys:    make([]coldata.Vec, len(eqCols)),
			buckets: make([]uint64, coldata.BatchSize()),
			groupID: make([]uint64, coldata.BatchSize()),
			headID:  make([]uint64, coldata.BatchSize()),
			toCheck: make([]uint64, coldata.BatchSize()),
			differs: make([]bool, coldata.BatchSize()),
		},

		vals:     allocator.NewMemBatchWithSize(keepTypes, 0 /* initialSize */),
		valTypes: keepTypes,
		valCols:  keepCols,
		keyTypes: keyTypes,
		keyCols:  keys,
		outCols:  outs,
		outTypes: outTypes,

		numBuckets: numBuckets,

		allowNullEquality: allowNullEquality,
		mode:              mode,
	}

	if mode == hashTableDistinctMode {
		ht.probeScratch.first = make([]uint64, numBuckets)
		ht.probeScratch.next = make([]uint64, coldata.BatchSize()+1)

		ht.buildScratch.next = make([]uint64, 1, coldata.BatchSize()+1)
		ht.probeScratch.hashBuffer = make([]uint64, coldata.BatchSize())
		ht.probeScratch.distinct = make([]bool, coldata.BatchSize())

		ht.probeScratch.last = make([]uint64, numBuckets)
		ht.probeScratch.prev = make([]uint64, coldata.BatchSize()+1)

		ht.buildScratch.last = make([]uint64, numBuckets)
	}

	return ht
}

// build executes the entirety of the hash table build phase using the input
// as the build source. The input is entirely consumed in the process.
func (ht *hashTable) build(ctx context.Context, input Operator) {
	nKeyCols := len(ht.keyCols)

	switch ht.mode {
	case hashTableFullMode:
		for {
			batch := input.Next(ctx)
			if batch.Length() == 0 {
				break
			}

			ht.loadBatch(batch)
		}

		keyCols := make([]coldata.Vec, nKeyCols)
		for i := 0; i < nKeyCols; i++ {
			keyCols[i] = ht.vals.ColVec(int(ht.keyCols[i]))
		}

		// ht.next is used to store the computed hash value of each key.
		ht.buildScratch.next = maybeAllocateUint64Array(ht.buildScratch.next, ht.vals.Length()+1)
		ht.computeBuckets(ctx, ht.buildScratch.next[1:], ht.keyTypes, keyCols, ht.vals.Length(), nil)
		ht.buildNextChains(ctx, ht.buildScratch.first, ht.buildScratch.next, 1, ht.vals.Length())
	case hashTableDistinctMode:
		for {
			batch := input.Next(ctx)
			if batch.Length() == 0 {
				break
			}

			srcVecs := batch.ColVecs()
			targetVecs := ht.vals.ColVecs()

			for i := 0; i < nKeyCols; i++ {
				ht.probeScratch.keys[i] = srcVecs[ht.keyCols[i]]
			}

			ht.computeBuckets(
				ctx, ht.probeScratch.hashBuffer, ht.keyTypes, ht.probeScratch.keys, batch.Length(), batch.Selection())
			ht.buildDoubleLinkedChains(
				ctx, ht.probeScratch.first, ht.probeScratch.last, ht.probeScratch.next,
				ht.probeScratch.prev, ht.probeScratch.hashBuffer, 1, batch.Length())

			ht.removeDuplicates(batch, ht.probeScratch.keys, ht.probeScratch.first, ht.probeScratch.next, ht.checkProbeForDistinct)

			if batch.Length() > 0 {
				// We only check duplicates when there is tuple buffered.
				if ht.vals.Length() > 0 {
					ht.removeDuplicates(batch, ht.probeScratch.keys, ht.buildScratch.first, ht.buildScratch.next, ht.checkBuildForDistinct)
				}

				if batch.Length() > 0 {
					ht.allocator.PerformOperation(targetVecs, func() {
						for i, colIdx := range ht.valCols {
							targetVecs[i].Append(
								coldata.SliceArgs{
									ColType:   ht.valTypes[i],
									Src:       srcVecs[colIdx],
									Sel:       batch.Selection(),
									DestIdx:   ht.vals.Length(),
									SrcEndIdx: batch.Length(),
								},
							)
						}
					})

					// rebuild next chain using filtered hash buffer
					ht.buildDoubleLinkedChains(
						ctx, ht.probeScratch.first, ht.probeScratch.last, ht.probeScratch.next,
						ht.probeScratch.prev, ht.probeScratch.hashBuffer, 1, batch.Length())

					// TODO(azhng): fix up
					originalBuildNextChainLen := len(ht.buildScratch.next) - 1
					ht.buildScratch.next = append(ht.buildScratch.next, ht.probeScratch.next[1:batch.Length()+1]...)

					if originalBuildNextChainLen == 0 {
						//copy(ht.buildScratch.last, ht.probeScratch.last)
						for _, hash := range ht.probeScratch.hashBuffer[:batch.Length()] {
							ht.buildScratch.first[hash] = ht.probeScratch.first[hash]
							ht.buildScratch.last[hash] = ht.probeScratch.last[hash]
						}
					} else {
						ht.mergeChain(ctx, ht.probeScratch.hashBuffer[:batch.Length()], uint64(originalBuildNextChainLen))
						for i := originalBuildNextChainLen + 1; i < len(ht.buildScratch.next); i++ {
							if ht.buildScratch.next[i] != 0 {
								ht.buildScratch.next[i] += uint64(originalBuildNextChainLen)
							}
						}
					}

					ht.vals.SetLength(ht.vals.Length() + batch.Length())
				}
			}
		}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("hashTable in unhandled state"))
	}
}

// removeDuplicates checks the tuples in the probe table against another table
// and updates the selection vector of the probe table to only include distinct
// tuples. The table removeDuplicates will check against is specified by
// `first`, `next` vectors and `duplicatesChecker`. `duplicatesChecker` takes
// a slice of key columns of the probe table, number of tuple to check and the
// selection vector of the probe table and returns number of tuples that needs
// to be checked for next iteration. It populates the ht.probeScratch.headID to
// point to the keyIDs that need to be included in probe table's selection
// vector.
// NOTE: *first* and *next* vectors should be properly populated.
func (ht *hashTable) removeDuplicates(
	batch coldata.Batch,
	keyCols []coldata.Vec,
	first, next []uint64,
	duplicatesChecker func([]coldata.Vec, uint64, []int) uint64,
) {
	nToCheck := uint64(batch.Length())
	sel := batch.Selection()

	for i := uint64(0); i < nToCheck; i++ {
		ht.probeScratch.groupID[i] = first[ht.probeScratch.hashBuffer[i]]
		ht.probeScratch.toCheck[i] = i
	}

	for nToCheck > 0 {
		// Continue searching for the build table matching keys while the toCheck
		// array is non-empty.
		nToCheck = duplicatesChecker(keyCols, nToCheck, sel)
		ht.findNext(next, nToCheck)
	}

	ht.updateSel(batch)
}

// checkCols performs a column by column checkCol on the key columns.
func (ht *hashTable) checkCols(
	probeVecs, buildVecs []coldata.Vec,
	probeKeyTypes []coltypes.T,
	buildKeyCols []uint32,
	nToCheck uint64,
	probeSel []int,
	buildSel []int,
) {
	for i := range ht.keyCols {
		probeType := probeKeyTypes[i]
		buildType := ht.keyTypes[i]
		ht.checkCol(probeVecs[i], buildVecs[buildKeyCols[i]], probeType, buildType,
			i, nToCheck, probeSel, buildSel)
	}
}

// checkColsForDistinctTuples performs a column by column check to find distinct
// tuples in the probe table that are not present in the build table.
func (ht *hashTable) checkColsForDistinctTuples(
	probeVecs []coldata.Vec, nToCheck uint64, probeSel []int,
) {
	buildVecs := ht.vals.ColVecs()
	for i := range ht.keyCols {
		probeVec := probeVecs[i]
		buildVec := buildVecs[ht.keyCols[i]]
		probeType := ht.keyTypes[i]

		ht.checkColForDistinctTuples(probeVec, buildVec, probeType, nToCheck, probeSel)
	}
}

// loadBatch appends a new batch of keys and outputs to the existing keys and
// output columns.
func (ht *hashTable) loadBatch(batch coldata.Batch) {
	batchSize := batch.Length()
	ht.allocator.PerformOperation(ht.vals.ColVecs(), func() {
		htSize := ht.vals.Length()
		for i, colIdx := range ht.valCols {
			ht.vals.ColVec(i).Append(
				coldata.SliceArgs{
					ColType:   ht.valTypes[i],
					Src:       batch.ColVec(int(colIdx)),
					Sel:       batch.Selection(),
					DestIdx:   htSize,
					SrcEndIdx: batchSize,
				},
			)
		}
		ht.vals.SetLength(htSize + batchSize)
	})
}

// computeBuckets computes the hash value of each key and stores the result in
// buckets.
func (ht *hashTable) computeBuckets(
	ctx context.Context,
	buckets []uint64,
	keyTypes []coltypes.T,
	keys []coldata.Vec,
	nKeys int,
	sel []int,
) {
	initHash(buckets, nKeys, defaultInitHashValue)

	if nKeys == 0 {
		// No work to do - avoid doing the loops below.
		return
	}

	for i := range ht.keyCols {
		rehash(ctx, buckets, keyTypes[i], keys[i], nKeys, sel, ht.cancelChecker, ht.decimalScratch)
	}

	finalizeHash(buckets, nKeys, ht.numBuckets)
}

// buildNextChain builds the hash map from the computed hash values.
func (ht *hashTable) buildNextChains(
	ctx context.Context, first, next []uint64, offset, batchSize int,
) {
	for id := offset; id < offset+batchSize; id++ {
		ht.cancelChecker.check(ctx)
		// keyID is stored into corresponding hash bucket at the front of the next
		// chain.
		hash := next[id]
		next[id] = first[hash]
		first[hash] = uint64(id)
	}
}

// TODO(azhng): doc
// buildDoubleLinkedChains builds the hash map from the computed hash values.
func (ht *hashTable) buildDoubleLinkedChains(
	ctx context.Context, first, last, next, prev, hashBuffer []uint64, offset, batchSize int,
) {
	for n := 0; n < len(ht.probeScratch.first); n += copy(ht.probeScratch.first[n:], zeroUint64Column) {
		copy(ht.probeScratch.last[n:], zeroUint64Column)
	}

	// TODO(azhng): ri -> reverse idx
	//              fi -> forward idx
	ht.cancelChecker.check(ctx)
	for ri := offset + batchSize - 1; ri >= offset; ri-- {
		fi := offset + batchSize - ri
		// keyID is stored into corresponding hash bucket at the front of the next
		// chain.
		fHash := hashBuffer[fi-1]
		rHash := hashBuffer[ri-1]

		next[ri] = first[rHash]
		prev[fi] = last[fHash]

		first[rHash] = uint64(ri)
		last[fHash] = uint64(fi)
	}
}

func (ht *hashTable) mergeChain(ctx context.Context, hashBuffer []uint64, offset uint64) {
	ht.cancelChecker.check(ctx)

	for _, hash := range hashBuffer {
		pFirstId := ht.probeScratch.first[hash] + offset
		pLastId := ht.probeScratch.last[hash] + offset
		if ht.buildScratch.last[hash] != 0 {
			if ht.buildScratch.last[hash] <= offset {
				ht.buildScratch.next[ht.buildScratch.last[hash]] = pFirstId
				ht.buildScratch.last[hash] = pLastId
			}
		} else {
			ht.buildScratch.first[hash] = pFirstId
			ht.buildScratch.last[hash] = pLastId
		}
	}
}

// maybeAllocate* methods make sure that the passed in array is allocated and
// of the desired length.
func maybeAllocateUint64Array(array []uint64, length int) []uint64 {
	if array == nil || cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroUint64Column) {
	}
	return array
}

func maybeAllocateBoolArray(array []bool, length int) []bool {
	if array == nil || cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroBoolColumn) {
	}
	return array
}

func (ht *hashTable) maybeAllocateSameAndVisited() {
	ht.same = maybeAllocateUint64Array(ht.same, ht.vals.Length()+1)
	ht.visited = maybeAllocateBoolArray(ht.visited, ht.vals.Length()+1)
	// Since keyID = 0 is reserved for end of list, it can be marked as visited
	// at the beginning.
	ht.visited[0] = true
}

// lookupInitial finds the corresponding hash table buckets for the equality
// column of the batch and stores the results in groupID. It also initializes
// toCheck with all indices in the range [0, batchSize).
func (ht *hashTable) lookupInitial(
	ctx context.Context, keyTypes []coltypes.T, batchSize int, sel []int,
) {
	ht.computeBuckets(ctx, ht.probeScratch.buckets, keyTypes, ht.probeScratch.keys, batchSize, sel)
	for i := 0; i < batchSize; i++ {
		ht.probeScratch.groupID[i] = ht.buildScratch.first[ht.probeScratch.buckets[i]]
		ht.probeScratch.toCheck[i] = uint64(i)
	}
}

// findNext determines the id of the next key inside the groupID buckets for
// each equality column key in toCheck.
func (ht *hashTable) findNext(next []uint64, nToCheck uint64) {
	for i := uint64(0); i < nToCheck; i++ {
		ht.probeScratch.groupID[ht.probeScratch.toCheck[i]] =
			next[ht.probeScratch.groupID[ht.probeScratch.toCheck[i]]]
	}
}

// reset resets the hashTable for reuse.
// NOTE: memory that already has been allocated for ht.vals is *not* released.
// However, resetting the length of ht.vals to zero doesn't confuse the
// allocator - it is smart enough to look at the capacities of the allocated
// vectors, and the capacities would stay the same until an actual new
// allocation is needed, and at that time the allocator will update the memory
// account accordingly.
func (ht *hashTable) reset() {
	for n := 0; n < len(ht.buildScratch.first); n += copy(ht.buildScratch.first[n:], zeroUint64Column) {
	}
	ht.buildScratch.next = ht.buildScratch.next[:1]

	ht.vals.ResetInternalBatch()
	ht.vals.SetLength(0)
	// ht.next, ht.same and ht.visited are reset separately before
	// they are used (these slices are not used in all of the code paths).
	// ht.buckets doesn't need to be reset because buckets are always initialized
	// when computing the hash.
	copy(ht.probeScratch.groupID[:coldata.BatchSize()], zeroUint64Column)
	// ht.toCheck doesn't need to be reset because it is populated manually every
	// time before checking the columns.
	copy(ht.probeScratch.headID[:coldata.BatchSize()], zeroUint64Column)
	copy(ht.probeScratch.differs[:coldata.BatchSize()], zeroBoolColumn)
	copy(ht.probeScratch.distinct, zeroBoolColumn)
}
