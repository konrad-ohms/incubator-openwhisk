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
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.event.Logging.{ErrorLevel, InfoLevel}
import pureconfig.loadConfigOrThrow

import scala.util.Try
import whisk.common.{Logging, LoggingMarkers, TransactionId}
import whisk.core.ConfigKeys
import whisk.core.containerpool.ContainerId
import whisk.core.containerpool.docker.ProcessRunner

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * Configuration for wskc client command timeouts.
  */
case class WskcClientTimeoutConfig(run: Duration,
                                   rm: Duration,
                                   inspect: Duration,
                                   pull: Duration,
                                   pause: Duration,
                                   unpause: Duration,
                                   init: Duration,
                                   cleanup: Duration)

/**
  * Configuration for wskc client
  */
case class WskcClientConfig(timeouts: WskcClientTimeoutConfig)

/**
  * Serves as interface to the wskc CLI tool which will use containerd and CNI to manage network and containers.
  *
  * Be cautious with the ExecutionContext passed to this, as the
  * calls to the CLI are blocking.
  *
  * Only one instance per invoker is needed.
  */
class ContainerdClient( config: WskcClientConfig = loadConfigOrThrow[WskcClientConfig](ConfigKeys.dockerClient))(
  executionContext: ExecutionContext)(
  implicit log: Logging, as: ActorSystem)
  extends WskcApi with ProcessRunner {
  implicit private val ec = executionContext

  // Determines how to run wskc. Failure to find the wskc binary implies
  // a failure to initialize this instance of ContainerdClient.
  protected val wskcCmd: Seq[String] = {
    val alternatives = List("/usr/local/bin/wskc", "/usr/bin/wskc", "/bin/wskc")

    val wskcBin = Try {
      alternatives.find(a => Files.isExecutable(Paths.get(a))).get
    } getOrElse {
      throw new FileNotFoundException(s"Couldn't locate wskc binary (tried: ${alternatives.mkString(", ")}).")
    }

    Seq(wskcBin, "--namespace", "wsk")
  }

  // wskc --namespace wsk init
  def init(): Future[Unit] = {
    implicit val transid = TransactionId.invoker
    runCmd(Seq("init"), config.timeouts.init).map(_ => ())
  }

  // wskc --namespace wsk cleanup
  def cleanup(): Future[Unit] = {
    implicit val transid = TransactionId.invoker
    runCmd(Seq("cleanup"), config.timeouts.cleanup).map(_ => ())
  }

  // wskc --namespace wsk run --memory=256 docker.io/openwhisk/action-nodejs-v8:latest mycontainer
  def run(image: String, name: String, args: Seq[String] = Seq.empty[String])(
    implicit transid: TransactionId): Future[ContainerId] = {
    blocking {
      runCmd(Seq("run") ++ args ++ Seq(image, name), config.timeouts.run)
        .map(_ => ContainerId(name))
        .recoverWith {
          case e =>
            log.error(this, s"Failed create container for '$name': ${e.getClass} - ${e.getMessage}")
            Future.failed(new Exception(s"Failed to create container '$name'"))
        }
    }
  }

  // wskc --namespace wsk pause mycontainer
  def pause(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("pause", id.asString), config.timeouts.pause).map(_ => ())

  // wskc --namespace wsk unpause mycontainer
  def unpause(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("unpause", id.asString), config.timeouts.unpause).map(_ => ())

  // wskc --namespace wsk rm mycontainer
  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("rm", id.asString), config.timeouts.rm).map(_ => ())

  // wskc --namespace wsk inspect --isOomKilled myContainer
  def isOomKilled(id: ContainerId)(implicit transid: TransactionId): Future[Boolean] =
    runCmd(Seq("inspect", "--isOomKilled", id.asString), config.timeouts.inspect).map(_.toBoolean)

  // wskc --namespace wsk network del mycontainer
  def deleteNetwork(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("network", "del", id.asString), config.timeouts.rm).map(_ => ())

  // wskc --namespace wsk network add mycontainer
  def createNetwork(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("network", "add", id.asString), config.timeouts.rm).map(_ => ())

  /**
    * Stores pulls that are currently being executed and collapses multiple
    * pulls into just one. After a pull is finished, the cached future is removed
    * to enable constant updates of an image without changing its tag.
    */
  // wskc --namespace wsk pull docker.io/openwhisk/action-nodejs-v8:latest
  private val pullsInFlight = TrieMap[String, Future[Unit]]()
  def pull(image: String)(implicit transid: TransactionId): Future[Unit] =
    pullsInFlight.getOrElseUpdate(image, {
      runCmd(Seq("pull", "docker.io/" + image), config.timeouts.pull).map(_ => ()).andThen {
        case _ => pullsInFlight.remove(image)
      }
    })

  private def runCmd(args: Seq[String], timeout: Duration)(implicit transid: TransactionId): Future[String] = {
    val cmd = wskcCmd ++ args
    val start = transid.started(
      this,
      LoggingMarkers.INVOKER_WSKC_CMD(args.head),
      s"running ${cmd.mkString(" ")} (timeout: $timeout)",
      logLevel = InfoLevel)
    executeProcess(cmd, timeout).andThen {
      case Success(_) => transid.finished(this, start)
      case Failure(t) => transid.failed(this, start, t.getMessage, ErrorLevel)
    }
  }
}

trait WskcApi {

  /**
    * Initialize a containerd namespace and creates shared files on disk
    */
  def init(): Future[Unit]

  /**
    * Cleanup a containerd namespace (removes all containers in namespace)
    */
  def cleanup(): Future[Unit]

  /**
    * Spawns a container in detached mode.
    *
    * @param image the image to start the container with
    * @param args arguments for the ctr run command
    * @return id of the started container
    */
  def run(image: String, name: String, args: Seq[String] = Seq.empty[String])(
    implicit transid: TransactionId): Future[ContainerId]

  /**
    * Pauses the container with the given id.
    *
    * @param id the id of the container to pause
    * @return a Future completing according to the command's exit-code
    */
  def pause(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  /**
    * Unpauses the container with the given id.
    *
    * @param id the id of the container to unpause
    * @return a Future completing according to the command's exit-code
    */
  def unpause(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  /**
    * Removes the container with the given id.
    *
    * @param id the id of the container to remove
    * @return a Future completing according to the command's exit-code
    */
  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  /**
    * Removes a device pair from a network bridge
    *
    * @param id the id of the network owning container to remove
    * @return a Future completing according to the command's exit-code
    */
  def deleteNetwork(id: ContainerId)(implicit  transid: TransactionId): Future[Unit]

  /**
    * Creates a device pair and connects it to a network bridge (creates bridge if necessary)
    *
    * @param id the id of the network owning container to remove
    * @return a Future completing according to the command's exit-code
    */
  def createNetwork(id: ContainerId)(implicit  transid: TransactionId): Future[Unit]

  /**
    * Pulls the given image.
    *
    * @param image the image to pull
    * @return a Future completing once the pull is complete
    */
  def pull(image: String)(implicit transid: TransactionId): Future[Unit]

  /**
    * Determines whether the given container was killed due to
    * memory constraints.
    *
    * @param id the id of the container to check
    * @return a Future containing whether the container was killed or not
    */
  def isOomKilled(id: ContainerId)(implicit transid: TransactionId): Future[Boolean]
}