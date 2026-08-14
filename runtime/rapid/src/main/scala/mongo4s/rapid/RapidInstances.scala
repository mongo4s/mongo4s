package mongo4s.rapid

trait RapidInstances extends TaskToEffectInstance, TaskToBridgeInstance

object RapidInstances extends RapidInstances
