package mongo4s.zio

trait ZioInstances extends TaskToEffectInstance, TaskToBridgeInstance

object ZioInstances extends ZioInstances
