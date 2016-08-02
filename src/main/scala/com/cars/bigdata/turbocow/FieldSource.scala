package com.cars.bigdata.turbocow

// Enum that describes all the possible places to pull a field from:
object FieldSource extends Enumeration {
    val Input,
        Enriched,
        Constant= Value
}

