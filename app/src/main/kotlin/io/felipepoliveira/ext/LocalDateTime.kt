package io.felipepoliveira.ext

import java.time.LocalDateTime
import java.time.YearMonth

fun LocalDateTime.countExcessDays(dayOfMonth: Int) =
    dayOfMonth - YearMonth.of(this.year, this.month.value).lengthOfMonth()

fun LocalDateTime.withDayOfMonthAndAdvanceMonthsOnExcess(dayOfMonthParam: Int): LocalDateTime {

    var dayOfMonth = dayOfMonthParam
    var excessDays = countExcessDays(dayOfMonth)

    // if there is no excess days, just set it
    if (excessDays < 0) {
        return this.withDayOfMonth(dayOfMonth)
    }

    // otherwise keep adding months until it reaches the end
    var refDate = this
    while (excessDays > 0) {
        refDate = refDate.plusMonths(1)
        dayOfMonth -= excessDays
        excessDays = refDate.countExcessDays(dayOfMonth)
    }

    excessDays = if (excessDays == 0) 1 else excessDays * -1

    return refDate.withDayOfMonth(excessDays)
}
