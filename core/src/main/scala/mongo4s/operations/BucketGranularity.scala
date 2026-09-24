package mongo4s.operations

enum BucketGranularity(val wireName: String):
  case R5         extends BucketGranularity("R5")
  case R10        extends BucketGranularity("R10")
  case R20        extends BucketGranularity("R20")
  case R40        extends BucketGranularity("R40")
  case R80        extends BucketGranularity("R80")
  case OneTwoFive extends BucketGranularity("1-2-5")
  case E6         extends BucketGranularity("E6")
  case E12        extends BucketGranularity("E12")
  case E24        extends BucketGranularity("E24")
  case E48        extends BucketGranularity("E48")
  case E96        extends BucketGranularity("E96")
  case E192       extends BucketGranularity("E192")
  case PowersOf2  extends BucketGranularity("POWERSOF2")
