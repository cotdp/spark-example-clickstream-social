package spark.example

// This is used as a value object where we store the data we're interested in

case class CandyCrushInteraction(
  id: String,
  user: String,
  level: Int,
  gender: String,
  language: String
)
