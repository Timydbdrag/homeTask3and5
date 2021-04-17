package entity

case class TaxiData(
                    trip_distance:Double,
                    PULocationID:Int,
                    DOLocationID:Int)

case class ResData(
                   district_id:String,
                   count_trips:Int,
                   avr:Double,
                   deviation:Double,
                   min_distance:Double,
                   max_distance:Double)



