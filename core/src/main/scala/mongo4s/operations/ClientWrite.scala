package mongo4s.operations

import com.mongodb.MongoNamespace
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel

final class ClientWrite private[mongo4s] (
    val namespace: MongoNamespace,
    private[mongo4s] val model: () => ClientNamespacedWriteModel,
)
