package io.felipepoliveira.ext

/**
 * Create a new instance of IntRange if the given IntRange intersects
 * with this. The new instance will be the minimum(this, another)..maximum(this.another). If
 * their do not intersect will return null
 */
fun IntRange.groupIfIntersects(another: IntRange): IntRange? {
    if (this.intersect(another).isEmpty())
        return null

    return group(another)

}

/**
 * Create a new instance of IntRange where `minimum(this, another)..maximum(this.another)`
 */
fun IntRange.group(another: IntRange): IntRange {
    return minOf(this.first, another.first)..maxOf(this.last, another.last)
}

fun List<IntRange>.groupIntersections(): List<IntRange> {

    // If there is not at least 2 elements returns this list
    if (this.size < 2)
        return this

    // this list will be used as the reference for the source list
    // this list will be mutated (remove items) every time the algorithm find ranges that can be grouped
    var rangeCopy = this.toMutableList()

    // everytime a range is matched in the reference range their index wil be added on this set
    // this will be used to remove the useless ranges
    val intersectedIndices = mutableSetOf<Int>()

    // this is the final array that will the store the normalized ranges
    val result = mutableListOf<IntRange>()
    do {

        // as the rangeCopy mutates, its size will change, so this verifications will assert correct use of
        // the algorithm:
        // if there is only one value, add it on the result and break the loop
        // if you are wondering if this is a deadlock, it is not because of `intersectedIndices`
        // always removes elements from `rangeCopy` and at least the element 0 its always added
        // as you can see below
        if (rangeCopy.size == 1) {
            result.add(rangeCopy[0])
            break
        }

        // this is the start of the algorithm. First the intersectedIndices array will always start at 0
        // as the first element (0) is used as ref
        intersectedIndices.add(0)
        var intersection = rangeCopy[0]

        // this variable will be the head that will check the ref range (0) to the others
        var comp = 1
        while (comp < rangeCopy.size) {


            val rangeToCompare = rangeCopy[comp]

            // if the current compared range was already included ignore it and go to the next one, but
            // only if it is not the last, as it has a special treatment at the end of the loop
            if (intersectedIndices.contains(comp) && comp < rangeCopy.size - 1) {
                comp += 1
                continue
            }

            // if the current range was not already verified and intersects with the ref range
            // add it on the intersected list and group it with the reference range
            // the comp will be reset to 1 as it can now include other elements after the first
            if (!intersectedIndices.contains(comp) && intersection.intersect(rangeToCompare).isNotEmpty()) {
                intersectedIndices.add(comp)
                intersection = intersection.group(rangeToCompare)
                comp =  1
                continue
            }

            // if the current element is the last and did intersect with the reference it will
            // remove from the rangeCopy all elements that was already found on intersection verifications
            // add the current intersection state in the result list and clear the intersected indices
            // as the rangeCopy was filtrated
            if (comp == rangeCopy.size - 1) {
                rangeCopy = rangeCopy
                    .withIndex()
                    .filter { !intersectedIndices.contains(it.index) }
                    .map { it.value }
                    .toMutableList()

                result.add(intersection)
                intersectedIndices.clear()
                break
            }
            comp += 1
        }
    } while(true)

    return result
}