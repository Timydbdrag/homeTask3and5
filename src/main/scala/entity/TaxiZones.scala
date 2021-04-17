package entity

case class TaxiZones(
                      LocationID: Int,
                      Borough: String
                    )

case class TaxiData2(
                      Borough: String,
                      trip_distance: Double,
                      PULocationID: Int,
                      DOLocationID: Int)
