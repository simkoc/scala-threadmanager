package de.halcony.threadmanager

import wvlet.log.LogLevel.ERROR
import wvlet.log.{LogLevel, LogSupport}

import scala.annotation.nowarn
import scala.collection.mutable.ListBuffer

/** Utility class to manage multiple threads processing jobs created via the ThreadManagerBuilder
  *
  * @tparam T the type of the jobs being processed by the managed threads
  */
class ThreadManager[T] extends LogSupport {

  private var threadCount: Int = Runtime.getRuntime.availableProcessors
  protected[threadmanager] def setThreadCount(count: Int): ThreadManager[T] =
    synchronized {
      threadCount = count
      this
    }

  private var shouldDieOnEmpty = false
  protected[threadmanager] def dieOnEmpty(): ThreadManager[T] = {
    shouldDieOnEmpty = true
    this
  }
  private[threadmanager] def getDieOnEmpty: Boolean = synchronized {
    this.shouldDieOnEmpty
  }

  type OnErrorT = (Option[T], Throwable) => Option[(Option[T], Throwable)]

  this.logger.setLogLevel(ERROR)

  /** set the log level for this class
    *
    * @param level the log level
    */
  def setLogLevel(level: LogLevel): Unit = this.logger.setLogLevel(level)

  private var lambda: Option[T => Unit] = None

  protected[threadmanager] def setLambda(
      lambda: T => Unit): ThreadManager[T] = {
    this.lambda = Some(lambda)
    this
  }
  private[threadmanager] def getJobLambda: T => Unit = this.lambda.get

  private val errors: ListBuffer[(Option[T], Throwable)] = new ListBuffer()
  def getErrors: Seq[(Option[T], Throwable)] = synchronized {
    errors.toList
  }
  @nowarn //this is an API call
  def resetErrors(): Seq[(Option[T], Throwable)] = synchronized {
    val ret: Seq[(Option[T], Throwable)] = getErrors
    errors.clear()
    ret
  }
  private var onError: OnErrorT = (input, thr) => {
    this.error(s"unhandled error while processing $input", thr)
    Some((input, thr))
  }
  protected[threadmanager] def setOnError(
      onError: OnErrorT): ThreadManager[T] = {
    this.onError = onError
    this
  }
  private[threadmanager] def encounteredError(input: Option[T],
                                              thr: Throwable): Unit = {
    onError(input, thr) match {
      case Some(err) =>
        synchronized {
          errors.addOne(err)
        }
      case None =>
    }
  }

  private var threadsActive = true
  private[threadmanager] def getThreadsActive: Boolean = synchronized {
    this.threadsActive
  }
  private[threadmanager] def setThreadsInactive(): ThreadManager[T] =
    synchronized {
      this.threadsActive = false
      this
    }
  @nowarn //this is an API call
  private[threadmanager] def setThreadsActive(): ThreadManager[T] =
    synchronized {
      this.threadsActive = true
      this
    }

  private val jobQueue: collection.mutable.Queue[T] = collection.mutable.Queue()
  def addJob(job: T): Unit = synchronized {
    jobQueue.addOne(job)
    this.notifyAll() // we need notify all as we might be waiting on ourself in waitFor
  }
  def addJobs(jobs: Seq[T]): Unit = synchronized {
    jobQueue.addAll(jobs)
    this.notifyAll()
  }
  def remainingJobs(): Int = synchronized {
    this.jobQueue.length
  }

  private val threads: collection.mutable.Set[ThreadManagerThread[T]] =
    collection.mutable.Set()
  private def areThreadsAlive(): Boolean = synchronized {
    threads.exists(!_.getThreadState.isInstanceOf[DoneThreadState])
  }
  @nowarn //this is an API call
  def getThreadJobs: Map[Int, Option[T]] = synchronized {
    threads.map { thread =>
      thread.getThreadIdentifier -> (thread.getThreadState match {
        case Working(job: T) => Some(job)
        case _               => None
      })
    }.toMap
  }


  private var paused: Boolean = false
  @nowarn //this is an API call
  def pauseThreads(waitOnPause: Boolean = true): Unit = synchronized {
    debug("pausing threads")
    this.paused = true
    if (waitOnPause) {
      while (threads.exists(!_.getThreadState.isInstanceOf[Stopped]) && this
               .threadsArePaused()) {
        this.wait()
        // wait for notification by the threads
      }
    }
  }

  @nowarn //this is an API call
  def unpauseThreads(): Unit = synchronized {
    debug("unpausing threads")
    this.paused = false
    this.notifyAll()
  }

  private[threadmanager] def threadsArePaused(): Boolean = synchronized {
    this.paused
  }

  /**
    *
    * @return
    */
  private[threadmanager] def getJobQueue = this.jobQueue

  @nowarn //this is an API call
  private[threadmanager] def threadDebug(id: Int, msg: String): Unit = {
    debug(s"[thread-$id] " + msg)
  }
  @nowarn //this is an API call
  private[threadmanager] def threadInfo(id: Int, msg: String): Unit = {
    info(s"[thread-$id] " + msg)
  }
  @nowarn //this is an API call
  private[threadmanager] def threadWarn(id: Int, msg: String): Unit = {
    warn(s"[thread-$id] " + msg)
  }
  @nowarn //this is an API call
  private[threadmanager] def threadError(id: Int, msg: String): Unit = {
    error(s"[thread-$id] " + msg)
  }

  protected[threadmanager] def createPool(): ThreadManager[T] = {
    (0 until threadCount).foreach { id =>
      threads.addOne(new ThreadManagerThread[T](id, this))
    }
    this
  }

  /** start the thread pool
    *
    * @return the current ThreadManager
    */
  def start(): ThreadManager[T] = {
    this.synchronized {
      this.threads.foreach(_.start())
    }
    this
  }

  /** stop all running jobs within a grace period
    *
    * @param gracePeriodMs the grace period (default 100ms)
    * @return whether all threads stopped
    */
  def stop(gracePeriodMs: Long = 100): Boolean = {
    synchronized {
      this.setThreadsInactive() // set shall be running to false
      this.notifyAll() // notify all threads in case they are waiting for input
    }
    val start = System.currentTimeMillis() // get the current time
    !this.threads
      .map { // go through all threads
        thread =>
          val delta = List((start + gracePeriodMs) - System.currentTimeMillis(),
                           0).max // get remaining grace period
          thread.join(delta) // try to join with thread within grace period
          thread.isAlive // check if thread is still alive
      }
      .exists(identity) // if any thread is still alive the join failed
  }

  /** check if there are still threads alive
    *
    * @return
    */
  def isAlive: Boolean = synchronized {
    this.areThreadsAlive()
  }

  /** stop all running threads via interrupts
    *
    */
  def destroy(): Unit = {
    this.threads.foreach { thread =>
      thread.interrupt()
    }
  }

  @nowarn //this is an API call
  def destroy(waitMs: Long): Unit = {
    val start = System.currentTimeMillis()
    destroy()
    threads.foreach { thread =>
      val delta = (start + waitMs) - System.currentTimeMillis()
      if (delta > 0) {
        thread.join(delta)
      } else {
        thread.join(1)
      }
    }
  }

  def destroyAndWait(): Unit = {
    destroy()
    threads.foreach {
      _.join()
    }
  }

  /** wait for the jobs to finish within a timeout
    *
    * @param timeoutMs the time for the job queue to be finished within
    */
  def waitFor(timeoutMs: Long): Boolean = {
    val start = System.currentTimeMillis() // get the current time
    var delta = (start + timeoutMs) - System
      .currentTimeMillis() // calculate the remaining time
    while (!threadsArePaused() && areThreadsAlive() && delta > 0) { // while there are still threads alive and time to wait
      delta = List((start + timeoutMs) - System
                     .currentTimeMillis(),
                   0).max // calculate the remaining time
      info(s"waiting for $delta")
      this.synchronized {
        if (delta > 0)
          this.wait(delta) // and wait for that time
      }
    }
    this.jobQueue.isEmpty // check if there are still jobs remaining
  }

  def waitFor(): Boolean = {
    try {
      while (!threadsArePaused() && areThreadsAlive()) { // while there are still threads alive
        this.synchronized {
          this.wait() // wait
        }
      }
      true
    } catch {
      case x: Throwable =>
        error(x)
        false
    }
  }

}
