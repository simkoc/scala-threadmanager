package de.halcony.threadmanager

import wvlet.log.LogLevel.ERROR
import wvlet.log.{LogLevel, LogSupport}

import scala.collection.mutable.ListBuffer

/** Utility class to manage multiple threads processing jobs created via the ThreadManagerBuilder
  *
  * @tparam T the type of the jobs being processed by the managed threads
  */
class ThreadManager[T] extends LogSupport {

  private var shouldDieOnEmpty = false

  protected[threadmanager] def dieOnEmpty() : ThreadManager[T] = {
    shouldDieOnEmpty = true
    this
  }

  type OnErrorT = (Option[T], Throwable) => Option[(Option[T], Throwable)]

  this.logger.setLogLevel(ERROR)

  /** set the log level for this class
    *
    * @param level the log level
    */
  def setLogLevel(level: LogLevel): Unit = this.logger.setLogLevel(level)

  private var lambda: Option[T => Unit] = None

  protected[threadmanager] def setLambda(lambda: T => Unit) = {
    this.lambda = Some(lambda)
  }

  private val errors: ListBuffer[(Option[T], Throwable)] = new ListBuffer()

  def getErrors: Seq[(Option[T], Throwable)] = synchronized {
    errors.toList
  }

  def resetErrors(): Seq[(Option[T], Throwable)] = synchronized {
    val ret: Seq[(Option[T], Throwable)] = getErrors
    errors.clear()
    ret
  }

  private var onError: OnErrorT = (input, thr) => {
    this.error(s"unhandled error while processing $input", thr)
    Some((input, thr))
  }

  protected[threadmanager] def setOnError(onError: OnErrorT): ThreadManager[T] = {
    this.onError = onError
    this
  }

  private def encounteredError(input: Option[T], thr: Throwable): Unit = {
    onError(input, thr) match {
      case Some(err) =>
        synchronized {
          errors.addOne(err)
        }
      case None =>
    }
  }

  private var threadsShallBeRunning = true
  private def getKeepRunning: Boolean = synchronized {
    threadsShallBeRunning && (this.remainingJobs() > 0 || !shouldDieOnEmpty)
  }

  private var threadCount: Int = Runtime.getRuntime.availableProcessors

  protected[threadmanager] def setThreadCount(count: Int): ThreadManager[T] =
    synchronized {
      threadCount = count
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

  private val threads: collection.mutable.Map[Int, Thread] =
    collection.mutable.Map()
  private val threadsJob: collection.mutable.Map[Int, Option[T]] =
    collection.mutable.Map()

  private def areThreadsAlive(): Boolean = synchronized {
    threads.values.exists(_.isAlive)
  }

  private def setThreadJob(id: Int, job: Option[T]) = synchronized {
    threadsJob.addOne(id, job)
  }

  def getThreadJobs: Map[Int, Option[T]] = synchronized {
    this.threadsJob.toMap
  }

  //todo: the mutexes could be optimized to be split between manager and job queue
  protected[threadmanager] def createPool(): ThreadManager[T] = {
    val parentManager = this
    (0 until threadCount).foreach { id =>
      threads.addOne(id -> new Thread(() => {
        val myId = id
        parentManager.info(s"thread $myId has started")
        try {
          while (parentManager.getKeepRunning) { // as long as the threads are supposed to run
            parentManager.synchronized { // sync with the parent
              if (jobQueue.isEmpty) { // check if the job queue has an element
                this.wait() // if not wait for any notification on parent
                None // and say there was no job
              } else {
                Some(jobQueue.dequeue()) // if there is a job deque and provide
              }
            } match {
              case Some(job) =>
                try { // if there was a job
                  parentManager.info(s"thread $myId starts processing job $job")
                  this.setThreadJob(myId, Some(job)) // set the job the thread is working on
                  this.lambda.get.apply(job) // run the process lambda
                } catch {
                  // in case of any issue catch and log
                  case thr: Throwable =>
                    parentManager.encounteredError(Some(job), thr)
                } finally {
                  // and set the current job to none
                  setThreadJob(myId, None)
                  parentManager.synchronized {
                    parentManager.notifyAll()
                  }
                } // rinse and repeat
              case None => // if there wasn't a job, go to the start of the loop
            }
          }
        } catch {
          case thr: Throwable => parentManager.encounteredError(None, thr)
        } finally {
          parentManager.info(s"thread $myId is done")
          parentManager.synchronized {
            parentManager.notifyAll()
          }
        }
      }))
    }
    this
  }

  /** start the thread pool
    *
    * @return the current ThreadManager
    */
  def start(): ThreadManager[T] = {
    this.threads.values.foreach(_.start())
    this
  }

  /** stop all running jobs within a grace period
    *
    * @param gracePeriodMs the grace period (default 100ms)
    * @return whether all threads stopped
    */
  def stop(gracePeriodMs: Long = 100): Boolean = {
    synchronized {
      threadsShallBeRunning = false // set shall be running to false
      this.notifyAll() // notify all threads in case they are waiting for input
    }
    val start = System.currentTimeMillis() // get the current time
    this.threads.values
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
    this.threads.values.foreach { thread =>
      thread.interrupt()
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
    while (areThreadsAlive() && delta > 0) { // while there are still threads alive and time to wait
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

  def waitFor() : Boolean = {
    try {
      while (areThreadsAlive()) { // while there are still threads alive
        this.synchronized {
            this.wait() // wait
        }
      }
      true
    } catch {
      case x : Throwable =>
        error(x)
        false
    }
  }

}
