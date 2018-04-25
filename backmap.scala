
/**
 * Takes in a vector and a range and returns the argmax for that range
 */
def argmaxRange(vector: Vector, i: Int, j: Int): Tuple2(Int,Int) = {
	var max = 0
	var index = 0
	for (k <- i to j) {
		if (vector.apply(k) > max) {
			max = vector.apply(k)
			index = k
		}
	}
	(index, max)
}

/**
 * Backmap the greatest similarity to the names
 */
val backmap = (row : IndexedRow) => {
	
}
