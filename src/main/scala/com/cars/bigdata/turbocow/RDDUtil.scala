package com.cars.bigdata.turbocow

import org.apache.spark.rdd.RDD

object RDDUtil {

  // todo move into additions
  def split[T](rdd: RDD[T], filter: (T) => Boolean): (RDD[T], RDD[T]) = {
    ( rdd.filter(filter), rdd.filter( (t: T) => !filter(t) ) )
  }

  implicit class RDDAdditions[T](val rdd: RDD[T]) {

    /** Helper function to quickly split an RDD into two by calling filter() twice.
      *
      * @return tuple of (positiveFilteredRDD, negativeFilteredRDD)
      */
    def split(filter: (T) => Boolean): (RDD[T], RDD[T]) = RDDUtil.split(rdd, filter)
  }


}


