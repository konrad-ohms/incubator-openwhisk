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
import java.time.Instant
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.entity.ActivationResponse.{ConnectionError, MemoryExhausted}
import org.apache.openwhisk.core.entity.{ActivationEntityLimit, ByteSize}
import org.apache.openwhisk.core.entity.size._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import spray.json._
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.http.Messages

object ContainerdContainer {
  /**
    * Creates a container running on a docker daemon.
    *
    * @param transid transaction creating the container
    * @param image either a user provided (Left) or OpenWhisk provided (Right) image
    * @param memory memorylimit of the container
    * @param cpuShares sharefactor for the container
    * @param environment environment variables to set on the container
    * @param name optional name for the container
    * @return a Future which either completes with a ContainerdContainer or one of two specific failures
    */
  def create(transid: TransactionId,
             image: Either[ImageName, String],
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
      "--memory-swap",
      s"${memory.toMB}")

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
      containerTuple <- wskc.run(imageToUse, cname, args).recoverWith {
        case _ =>
          // Iff the pull was successful, we assume that the error is not due to an image pull error, otherwise
          // the docker run was a backup measure to try and start the container anyway. If it fails again, we assume
          // the image could still not be pulled and wasn't available locally.
          if (pullSuccessful) {
            Future.failed(WhiskContainerStartupError(Messages.resourceProvisionError))
          } else {
            Future.failed(BlackboxStartupError(Messages.imagePullError(imageToUse)))
          }
      }
    } yield new ContainerdContainer(containerTuple._1, containerTuple._2)
  }
}

/**
  * Represents a container as run by containerd.
  *
  * @constructor
  * @param id the id of the container
  * @param addr the ip of the container
  */
class ContainerdContainer(protected val id: ContainerId, protected val addr: ContainerAddress)(
  implicit wskc: WskcApi,
  override protected val as: ActorSystem,
  protected val ec: ExecutionContext,
  protected val logging: Logging)
    extends Container {

  protected val waitForOomState: FiniteDuration = 2.seconds
  protected val filePollInterval: FiniteDuration = 5.milliseconds

  override def suspend()(implicit transid: TransactionId): Future[Unit] = {
    super.suspend()
    wskc.pause(id)
  }
  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    super.destroy()
    wskc.rm(id)
  }

  def resume()(implicit transid: TransactionId): Future[Unit] = wskc.unpause(id)

  // TODO solve logging via containerd, currently it does not write into files nor pipes
  def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Source[ByteString, Any] = {
    Source.empty[ByteString]
  }

  /**
    * Was the container killed due to memory exhaustion?
    *
    * @param retries number of retries to make
    * @return a Future indicating a memory exhaustion situation
    */
  private def isOomKilled(retries: Int = (waitForOomState / filePollInterval).toInt)(
    implicit transid: TransactionId): Future[Boolean] = {
    wskc.isOomKilled(id)(TransactionId.invoker).flatMap { killed =>
      if (killed) Future.successful(true)
      else if (retries > 0) akka.pattern.after(filePollInterval, as.scheduler)(isOomKilled(retries - 1))
      else Future.successful(false)
    }
  }

  override protected def callContainer(path: String, body: JsObject, timeout: FiniteDuration, maxConcurrent: Int, retry: Boolean = false)(
    implicit transid: TransactionId): Future[RunResult] = {
    val started = Instant.now()
    val http = httpConnection.getOrElse {
      val conn = if (Container.config.akkaClient) {
        new AkkaContainerClient(addr.host, addr.port, timeout, ActivationEntityLimit.MAX_ACTIVATION_ENTITY_LIMIT, 1024)
      } else {
        new ApacheBlockingContainerClient(
          s"${addr.host}:${addr.port}",
          timeout,
          ActivationEntityLimit.MAX_ACTIVATION_ENTITY_LIMIT,
          maxConcurrent)
      }
      httpConnection = Some(conn)
      conn
    }

    http
      .post(path, body, retry)
      .flatMap { response =>
        val finished = Instant.now()

        response.left
          .map {
            // Only check for memory exhaustion if there was a
            // terminal connection error.
            case error: ConnectionError =>
              isOomKilled().map {
                case true  => MemoryExhausted()
                case false => error
              }
            case other => Future.successful(other)
          }
          .fold(_.map(Left(_)), right => Future.successful(Right(right)))
          .map(res => RunResult(Interval(started, finished), res))
      }
  }
}
