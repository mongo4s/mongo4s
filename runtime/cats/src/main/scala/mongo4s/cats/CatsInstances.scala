package mongo4s.cats

trait CatsInstances extends AsyncToEffectInstance, AsyncToBridgeInstance

object CatsInstances extends CatsInstances
