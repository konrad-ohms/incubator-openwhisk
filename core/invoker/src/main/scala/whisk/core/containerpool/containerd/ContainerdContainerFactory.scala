package whisk.core.containerpool.containerd

import akka.actor.ActorSystem
import akka.japi.Option.Some

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.containerpool.Container
import whisk.core.containerpool.ContainerFactory
import whisk.core.containerpool.ContainerFactoryProvider
import whisk.core.entity.ByteSize
import whisk.core.entity.ExecManifest
import whisk.core.entity.InvokerInstanceId
import pureconfig._
import whisk.core.ConfigKeys
import whisk.core.containerpool.ContainerArgsConfig

class ContainerdContainerFactory(instance: InvokerInstanceId,
                                 parameters: Map[String, Set[String]],
                                 containerArgsConfig: ContainerArgsConfig =
                                 loadConfigOrThrow[ContainerArgsConfig](ConfigKeys.containerArgs))(
  implicit actorSystem: ActorSystem,
  ec: ExecutionContext,
  logging: Logging
) extends ContainerFactory {
  /** Initialize container clients */
  implicit val wskc = new ContainerdClient()(ec)

  override def init(): Unit = {
    try {
      wskc.cleanup()
    } catch {
      case e: Exception => logging.debug(this, s"Failed to cleanup containerd namespace before initializing, might be already clean: ${e.getMessage}")
    }
    try {
      wskc.init()
    } catch {
      case e: Exception => logging.error(this, s"Failed to initialize containerd namespace: ${e.getMessage}")
    }
  }

  override def cleanup(): Unit = {
    try {
      wskc.cleanup()
    } catch {
      case e: Exception => logging.error(this, s"Failed to cleanup containerd namespace: ${e.getMessage}")
    }
  }

  override def createContainer(tid: TransactionId,
                               name: String,
                               actionImage: ExecManifest.ImageName,
                               userProvidedImage: Boolean,
                               memory: ByteSize,
                               cpuShares: Int)(
    implicit config: WhiskConfig,
    logging: Logging): Future[Container] = {
    ContainerdContainer.create(
      tid,
      image = if (userProvidedImage) Left(actionImage) else Right(actionImage.localImageName(config.runtimesRegistry)),
      memory = memory,
      cpuShares = cpuShares,
      environment = Map("__OW_API_HOST" -> config.wskApiHost),
      name = Some(name)
    )
  }
}

object ContainerdContainerFactoryProvider extends ContainerFactoryProvider {
  override def instance(actorSystem: ActorSystem,
                        logging: Logging,
                        config: WhiskConfig,
                        instanceId: InvokerInstanceId,
                        parameters: Map[String, Set[String]]): ContainerFactory = {

    new ContainerdContainerFactory(instanceId, parameters)(
      actorSystem,
      actorSystem.dispatcher,
      logging)
  }
}