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

package whisk.core.containerpool.containerd
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import whisk.common.{Logging, TransactionId}
import whisk.core.containerpool._
import whisk.core.entity.ByteSize
import whisk.core.entity.size._
import whisk.http.Messages

import scala.concurrent.{ExecutionContext, Future}

object ContainerdContainer {
  def create(transid: TransactionId,
             image: String,
             userProvidedImage: Boolean = false,
             memory: ByteSize = 256.MB,
             cpuShares: Int = 0,
             environment: Map[String, String] = Map.empty,
             network: String = "bridge",
             dnsServers: Seq[String] = Seq.empty,
             name: Option[String] = None,
             dockerRunParameters: Map[String, Set[String]])(implicit wskc: WskcApi,
                                                            as: ActorSystem,
                                                            ec: ExecutionContext,
                                                            log: Logging): Future[ContainerdContainer] = {
    implicit val tid: TransactionId = transid

    val environmentArgs = environment.flatMap {
      case (key, value) => Seq("--env", s"$key=$value")
    }.toSeq

    for {
      cname <- name
        .map(n => Future.successful(n))
        .getOrElse(Future.failed(WhiskContainerStartupError("No name specified")))
      _ <- if (userProvidedImage) {
        wskc.pull(image).recoverWith {
          case _ => Future.failed(BlackboxStartupError(Messages.imagePullError(image)))
        }
      } else Future.successful(())
      id <- wskc.run(image, cname, environmentArgs).recoverWith {
        case _ =>
          Future.failed(WhiskContainerStartupError(Messages.resourceProvisionError))
      }
    } yield new ContainerdContainer(id, ContainerAddress("127.0.0.1")) //TODO get real IP here
  }
}

class ContainerdContainer(protected val id: ContainerId, protected val addr: ContainerAddress)(
  implicit wskc: WskcApi,
  override protected val as: ActorSystem,
  protected val ec: ExecutionContext,
  protected val logging: Logging)
    extends Container {

  override def suspend()(implicit transid: TransactionId): Future[Unit] = wskc.pause(id)
  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    super.destroy()
    wskc.rm(id)
  }
  def resume()(implicit transid: TransactionId): Future[Unit] = wskc.unpause(id)
  def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Source[ByteString, Any] = {
    Source.empty[ByteString]
  }

}
