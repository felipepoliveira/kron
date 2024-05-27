package io.felipepoliveira.commons.time

import io.felipepoliveira.ext.countExcessDays
import io.felipepoliveira.ext.groupIntersections
import io.felipepoliveira.ext.withDayOfMonthAndAdvanceMonthsOnExcess
import kotlinx.coroutines.*
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset

data class CronJobIsExecutingEvent (
    val startedAt: LocalDateTime,
    val nextExecutionAt: LocalDateTime,
)

/**
 * Parse a cron unit value to its integer value
 */
private fun parseCronUnitValueToInt(param: String, unit: CronTimeUnits): Int {
    if (unit == CronTimeUnits.DAY_OF_WEEK) {
        when (param.lowercase()) {
            "sunday", "sun" -> return 0
            "mon", "monday" -> return 1
            "tuesday", "tue" -> return 2
            "wednesday", "wed" -> return 3
            "thursday", "thu" -> return 4
            "friday", "fri" -> return 5
            "saturday", "sat" -> return 6
        }
    }
    else if (unit == CronTimeUnits.MONTH) {
        when (param.lowercase()) {
            "january", "jan" -> return 1
            "february", "feb" -> return 2
            "march", "mar" -> return 3
            "april", "apr" -> return 4
            "may" -> return 5
            "june", "jun" -> return 6
            "july", "jul" -> return 7
            "august", "aug" -> return 8
            "september", "sep" -> return 9
            "october", "oct" -> return 10
            "november", "nov" -> return 11
            "december", "dec" -> return 12
        }

    }

    return param.toInt()
}

/**
 * The main class of this file. This represents a cron expression. With this class it is possible
 * to schedule tasks that will execute continuously on a time-based period. To create a instance of this class
 * is recommended to use Cron.fromExpression() where is much easier to understand
 */
class Cron (
    /**
     * Contains data about the minute time unit of Cron
     */
    val minute: CronTimeUnit,
    /**
     * Contains data about the hour time unit of Cron
     */
    val hour: CronTimeUnit,
    /**
     * Contains data about the day of month time unit of Cron
     */
    val dayOfMonth: CronTimeUnit,
    /**
     * Contains data about the month time unit of Cron
     */
    val month: CronTimeUnit,
    /**
     * Contains data about the day of week time unit of Cron
     */
    val dayOfWeek: CronTimeUnit,
) {

    companion object {
        /**
         * Create a instance of Cron based on a unix cron format. You can learn more about the cron format in:
         * https://en.wikipedia.org/wiki/Cron or, for a more dynamic learning: https://cron.help/
         * @param cronInput: A String in unix/cron format, like 0 /8 * * 1-5 (every day at 00:00, 08:00, 16:00)
         * from monday to friday
         * @param timeZoneOffset: The timezone used on cron. Default value: UTC
         */
        fun fromExpression(cronInputParam: String): Cron {

            val cronInput = when(cronInputParam) {
                "@yearly", "@annually"  -> "0 0 1 1 *"
                "@monthly" -> "0 0 1 * *"
                "@weekly" -> "0 0 * * 0"
                "@daily", "@midnight" -> "0 0 * * *"
                "@hourly" -> "0 * * * *"
                else -> cronInputParam
            }

            // interpret as 5 part cron input
            val fivePartsCron = cronInput.split(" ").filter { i -> i.trim().isNotEmpty() }

            // assert that cron has 5 parts as the example: '* /1 * * *'
            if (fivePartsCron.size != 5) {
                throw IllegalArgumentException("The input should be a valid cron format like '* */4 * * MON-FRI'. $cronInput given")
            }

            return Cron(
                minute = CronTimeUnit(fivePartsCron[0], CronTimeUnits.MINUTE),
                hour = CronTimeUnit(fivePartsCron[1], CronTimeUnits.HOUR),
                dayOfMonth = CronTimeUnit(fivePartsCron[2], CronTimeUnits.DAY_OF_MONTH),
                month = CronTimeUnit(fivePartsCron[3], CronTimeUnits.MONTH),
                dayOfWeek = CronTimeUnit(fivePartsCron[4], CronTimeUnits.DAY_OF_WEEK),
            )
        }
    }

    /**
     * Return a flag indicating if the given LocalDateTime is consistent with all cron time units (minute, hour,
     * day of month, month and day of week)
     */
    fun onCron(refDate: LocalDateTime): Boolean {
        return (this.minute.onCron(refDate.minute) &&
            this.hour.onCron(refDate.hour) &&
            this.dayOfMonth.onCron(refDate.dayOfMonth) &&
            this.month.onCron(refDate.month.value) &&
            this.dayOfWeek.onCron(refDate.dayOfWeek.value))
    }

    /**
     * This function will receive the given LocalDateTime if it is on cron of the 'day of the week' field it will
     * return itself, otherwise it will keep adding days until it reaches the day of the week supported by the time unit
     */
    private fun currentOrNextTimeUntilValidDayOfWeek(refDate: LocalDateTime): LocalDateTime {
        if (this.dayOfWeek.onCron(refDate.dayOfWeek.value))
            return refDate

        var nextDate = refDate

        // day of week
        // next will be: next or the first (if there is no next)
        var next : Int? = this.dayOfWeek.next(nextDate.dayOfWeek.value) ?: this.dayOfWeek.first()

        // will advance days until it reaches the next day of week
        while ((nextDate.dayOfWeek.value) != next) {
            nextDate = nextDate.plusDays(1)
        }

        return nextDate
    }

    /**
     * This function will return a LocalDateTime that is cron consistent with this cron instance with time set after
     * the given LocalDateTime
     */
    fun nextTimeAfter(refDate: LocalDateTime): LocalDateTime {

        // the nextDate will be manipulated to generate the next date
        var nextDate = currentOrNextTimeUntilValidDayOfWeek(refDate)

        // remove seconds and millis
        nextDate = nextDate.withSecond(0)
        nextDate = nextDate.withNano(0)

        // minute
        var next = this.minute.next(nextDate.minute)
        if (next != null) {
            nextDate = nextDate.withMinute(next)
            if (onCron(nextDate)) return nextDate
        }
        // reset minute to the first value
        nextDate = nextDate.withMinute(this.minute.first())

        // hour
        next = this.hour.next(nextDate.hour)
        if (next != null) {
            nextDate = nextDate.withHour(next)
            if (onCron(nextDate)) return nextDate
        }
        // reset hour to the first value
        nextDate = nextDate.withHour(this.hour.first())

        // day of month
        next = this.dayOfMonth.next(nextDate.dayOfMonth)
        if (next != null && nextDate.countExcessDays(next) <= 0) {
            nextDate = currentOrNextTimeUntilValidDayOfWeek(nextDate.withDayOfMonth(next))
            if (onCron(nextDate)) return nextDate
        }

        // this will assert that the first day of month does not exceed the length of days in month in `nextDate`
        val firstDayOfMonthOnCron = this.dayOfMonth.first()
        val excessDays = nextDate.countExcessDays(firstDayOfMonthOnCron)

        // if the days exceed return
        if (excessDays > 0) {
            nextDate = currentOrNextTimeUntilValidDayOfWeek(nextDate.withDayOfMonthAndAdvanceMonthsOnExcess(excessDays))
            if (onCron(nextDate)) return nextDate
        }
        // reset day of month
        nextDate = nextDate.withDayOfMonth(firstDayOfMonthOnCron)

        // month
        next = this.month.next(nextDate.month.value)
        if (next != null) {
            nextDate = currentOrNextTimeUntilValidDayOfWeek(nextDate.withMonth(next))
            if (onCron(nextDate)) return nextDate
        }

        // reset month to the first value
        // also add a year, because the year has ended :)
        nextDate = nextDate.withMonth(this.dayOfMonth.first())
        nextDate = nextDate.plusYears(1)

        return currentOrNextTimeUntilValidDayOfWeek(nextDate)
    }

    /**
     * Start a worker that will run asynchronously based on a schedule of the cron. This worker will never start immediaylu,
     * it will use the function nextTimeAfter() to get the next validation date after now. From each execution it will
     * increment the timer always using the next of the current one
     * @param scope - The coroutine execution scope
     * @param execute - The function that will be triggered in each execution
     */
    fun startWorker(scope: CoroutineScope, execute: (event: CronJobIsExecutingEvent) -> Unit): Job {
        return scope.launch {

            // declare the timers
            var currentExecutionStartAt = nextTimeAfter(LocalDateTime.now())
            var nextExecutionStartAt = nextTimeAfter(currentExecutionStartAt)

            while (true) {

                // wait until the first execution
                val timeToWaitInMillis = Duration.between(LocalDateTime.now(), currentExecutionStartAt).toMillis()
                if (timeToWaitInMillis > 0) {
                    delay(timeToWaitInMillis)
                }

                // on interrupt...
                if (!isActive) {
                    return@launch
                }

                // execute it
                execute(CronJobIsExecutingEvent(
                    startedAt = currentExecutionStartAt,
                    nextExecutionAt = nextExecutionStartAt
                ))

                // calculate the next execution
                currentExecutionStartAt = nextExecutionStartAt
                nextExecutionStartAt = nextTimeAfter(currentExecutionStartAt)
            }
        }
    }

    override fun toString(): String {
        return "Cron(minute=$minute, hour=$hour, dayOfMonth=$dayOfMonth, month=$month, dayOfWeek=$dayOfWeek)"
    }
}

class CronTimeUnit(
    /**
     * Store the static values of the cron time unit
     */
    private var values: MutableList<Int>,
    /**
     * Store the ranges of the cron time unit
     */
    private var ranges: MutableList<IntRange>,
    /**
     * Store witch unit its using
     */
    val unit: CronTimeUnits
) {

    /**
     * This is a very important property in the class. It will include a sorted version of both values and ranges
     * arrays. It is used on the .next() function to find the next value of the time unit. Also everytime a value
     * inside .values array collides with a range on .ranges array it will be removed and replaced by the range.
     * For example:
     * If the values is [20, 15, 10, 5]
     * and ranges is [1..12, 36..60]
     * this array will be:
     * [1..12, 15, 20, 36..60]
     * You can see that the values [5, 10] was collided in 1..12 range
     * This collision and sort operations is made by private function setupProperties
     */
    private var sortedValuesAndRanges = mutableListOf<Any>()

    companion object {
        val VALUE_REGEX = Regex("^\\*|(\\d|\\w)+\$")
        val RANGE_REGEX = Regex("^(\\d|\\w)+-(\\d|\\w)+\$")
        val STEP_REGEX = Regex("^(\\*|(\\d|\\w)+)/((\\d|\\w)+)\$")
    }

    init {
        setupProperties()
    }

    constructor(cronInputValue: String, unit: CronTimeUnits) : this(
        values = mutableListOf(),
        ranges = mutableListOf(),
        unit = unit
    ) {

        // primary input validation
        if (cronInputValue.isEmpty()) {
            throw IllegalArgumentException("Cron time unit can not be empty")
        }

        val unitRange = unit.range()

        // split each statement from cron by ',' delimiter
        for (inputStmt in cronInputValue.split(",")) {

            // value: * or digit
            if (inputStmt.matches(VALUE_REGEX)) {
                if (inputStmt == "*") {
                    this.ranges.addLast(unit.range())
                }
                else {
                    this.values.add(parseCronUnitValueToInt(inputStmt, unit))
                }
            }
            // range: digit-digit
            else if (inputStmt.matches(RANGE_REGEX)) {
                val splitRangeStr = inputStmt.split("-")
                val initialRangeValue = parseCronUnitValueToInt(splitRangeStr[0], unit)
                val endRangeValue = parseCronUnitValueToInt(splitRangeStr[1], unit)

                if (initialRangeValue > endRangeValue) {
                    throw IllegalArgumentException("On range expression $inputStmt the initial value should be <= end value")
                }

                this.ranges.addLast(initialRangeValue..endRangeValue)
            }
            // step: *|digit/digit
            else if (inputStmt.matches(STEP_REGEX)) {
                // this expression basically means 'all values' so we add the range to use less memory
                if (inputStmt == "*/1") {
                    this.ranges.addLast(unit.range())
                }
                else {
                    val splitRangeStr = inputStmt.split("/")
                    val initialStepValue = if (splitRangeStr[0] == "*") unitRange.first else parseCronUnitValueToInt(splitRangeStr[0], unit)
                    val stepValue = parseCronUnitValueToInt(splitRangeStr[1], unit)
                    this.values.addAll(unit.steps(initialStepValue, stepValue))
                }
            }
            else {
                throw IllegalArgumentException("Unexpected expression $cronInputValue")
            }
        }

        // exceptions for DAY_OF_WEEK
        if (unit == CronTimeUnits.DAY_OF_WEEK) {

            // 7 and 0 are the same (SUNDAY) so it will assert that if one of them are on the cron but not the other
            // they will be added
            if (onCron(0) && !onCron(7))
                this.values.add(7)
            else if (onCron(7) && onCron(0))
                this.values.add(0)
        }

        // after interpretation algorithms...
        setupProperties()
    }

    fun first(): Int {
        return when (val first = sortedValuesAndRanges[0]) {
            is Int -> first
            is IntRange -> first.first
            else -> throw unsupportedTypeForSortedArray(first.javaClass)
        }
    }

    fun onCron(value: Int) = onValue(value) || onRange(value)

    fun onValue(value: Int) = this.values.contains(value)

    fun onRange(value: Int): Boolean {
        // if the value is contained in a range return true
        if (this.ranges.any { r -> value in r }) {
            return true
        }

        return false
    }

    fun next(value: Int): Int? {

        // for example:
        // value: 30
        // units: 0, 10..15, 40
        // the next value should be 40, as it the next value on the domain after 30

        // the algorithm will get the last element to check if there is a next element after the given value
        // if there is not, return null
        when (val lastElement = this.sortedValuesAndRanges.last()) {
            is Int -> if (value >= lastElement) return null
            is IntRange -> if (value >= lastElement.last) return null
        }

        // so, if the code reaches here it means that exists and candidate to be the next after the given value
        for (elem in this.sortedValuesAndRanges) {
            when (elem) {
                is Int -> if (value < elem) return elem
                is IntRange -> if (value < elem.first) else if (value < elem.last) return value + 1
            }
        }

        // the code should not reach this
        return null
    }

    private fun setupProperties() {
        // resolve range collisions
        this.ranges = this.ranges.groupIntersections()
            .toMutableList()

        // use an iterator as the value will be deleted from collection
        val valuesIter = this.values.iterator()
        while (valuesIter.hasNext()) {
            val value = valuesIter.next()

            // if the value is contained in a range remove it
            if (this.ranges.any { r -> value in r }) {
                valuesIter.remove()
            }
        }

        // sort ranges and values
        this.ranges = this.ranges.sortedBy { r -> r.first }.toMutableList()
        this.values = this.values.sorted().toMutableList()

        // populate the ordered values and ranges sorted
        sortedValuesAndRanges.addAll(ranges)
        sortedValuesAndRanges.addAll(values)
        sortedValuesAndRanges = sortedValuesAndRanges.sortedBy { o ->
            if (o is Int) o
            else if (o is IntRange) o.first
            else throw unsupportedTypeForSortedArray(o.javaClass)
        }.toMutableList()
    }

    private fun unsupportedTypeForSortedArray(type: Class<Any>): Exception =
        Exception("Unexpected error: Unsupported type found while creating ordered values and ranges: ${type.javaClass.name}")

    override fun toString(): String {
        return "CronTimeUnit(values=$values, ranges=$ranges, unit=$unit)"
    }
}

enum class CronTimeUnits {
    MINUTE,
    HOUR,
    DAY_OF_MONTH,
    MONTH,
    DAY_OF_WEEK
    ;

    fun range(): IntRange {
        return when (this) {
            MINUTE -> 0..59
            HOUR -> 0..23
            DAY_OF_MONTH -> 1..31
            MONTH -> 1..12
            DAY_OF_WEEK -> 0..7
        }
    }

    fun steps(initialValue: Int, step: Int): List<Int> {
        // assert that step is always an incremental value
        if (step < 1) {
            throw IllegalArgumentException("Step should be >= 1. $step given")
        }

        // the range will control when the steps should stop
        val range = this.range()
        if (initialValue < range.first) {
            throw IllegalArgumentException("The given initialValue $initialValue should be <= than the unit range first value ${range.first} for unit $this")
        }
        if (initialValue > range.last) {
            throw IllegalArgumentException("The given initialValue $initialValue should be <= than the unit range last value ${range.last} for unit $this")
        }

        // create the list that will be returned and add the first step of the range as the value
        val values = mutableListOf<Int>()
        var currentStep = initialValue
        values.addLast(currentStep)

        // increment the head currentStep and add its values into the array until it reaches or surpass the last value
        currentStep += step
        while (currentStep <= range.last) {
            values.addLast(currentStep)
            currentStep += step
        }

        return values
    }
}