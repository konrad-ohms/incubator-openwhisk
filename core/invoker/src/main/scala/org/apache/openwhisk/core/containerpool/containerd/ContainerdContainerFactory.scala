/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool.containerd

import akka.actor.ActorSystem
import akka.japi.Option.Some

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.containerpool.Container
import org.apache.openwhisk.core.containerpool.ContainerFactory
import org.apache.openwhisk.core.containerpool.ContainerFactoryProvider
import org.apache.openwhisk.core.entity.ByteSize
import org.apache.openwhisk.core.entity.ExecManifest
import org.apache.openwhisk.core.entity.InvokerInstanceId
import pureconfig._
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.containerpool.ContainerArgsConfig

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
    wskc.cleanup()
      .andThen {
        // Ensure that init is not done before cleanup has finished
        case _ => wskc.init()
      }
  }

  override def cleanup(): Unit = {
    wskc.cleanup()
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