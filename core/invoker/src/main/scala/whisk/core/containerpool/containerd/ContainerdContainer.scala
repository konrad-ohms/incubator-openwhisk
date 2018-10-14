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
import whisk.core.entity.ExecManifest.ImageName

import scala.concurrent.{ExecutionContext, Future}

object ContainerdContainer {
  def create(transid: TransactionId,
             image: Either[ImageName, String],
             userProvidedImage: Boolean = false,
             memory: ByteSize = 256.MB,
             cpuShares: Int = 0,
             environment: Map[String, String] = Map.empty,
             name: Option[String] = None)(implicit wskc: WskcApi,
                                                    as: ActorSystem,
                                                    ec: ExecutionContext,
                                                    log: Logging): Future[ContainerdContainer] = {
    implicit val tid: TransactionId = transid

    val environmentArgs = environment.flatMap {
      case (key, value) => Seq("--env", s"$key=$value")
    }.toSeq


    val args = Seq(
      "--cpu-shares",
      cpuShares.toString,
      "--memory",
      s"${memory.toMB}",
      environmentArgs ++
      name.map(n => Seq("--name", n)).getOrElse(Seq.empty))

    val imageToUse = image.fold(_.publicImageName, identity)

    val pulled = image match {
      case Left(userProvided) if userProvided.tag.map(_ == "latest").getOrElse(true) =>
        // Iff the image tag is "latest" explicitly (or implicitly because no tag is given at all), failing to pull will
        // fail the whole container bringup process, because it is expected to pick up the very latest "untagged"
        // version every time.
        wskc.pull(imageToUse).map(_ => true).recoverWith {
          case _ => Future.failed(BlackboxStartupError(Messages.imagePullError(imageToUse)))
        }
      case Left(_) =>
        // Iff the image tag is something else than latest, we tolerate an outdated image if one is available locally.
        // A `docker run` will be tried nonetheless to try to start a container (which will succeed if the image is
        // already available locally)
        wskc.pull(imageToUse).map(_ => true).recover { case _ => false }
      case Right(_) =>
        // Iff we're not pulling at all (OpenWhisk provided image) we act as if the pull was successful.
        Future.successful(true)
    }



    for {
      pullSuccessful <- pulled
      cname <- name
        .map(n => Future.successful(n))
        .getOrElse(Future.failed(WhiskContainerStartupError("No name specified")))
      id <- wskc.run(imageToUse, cname, environmentArgs).recoverWith {
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
