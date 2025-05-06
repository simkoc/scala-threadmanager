package de.halcony.threadmanager

import wvlet.log.LogLevel

import scala.annotation.nowarn

/** Utility to build a thread manager that uses the provided lambda to process incoming jobs
  *
  * @param lambda the lambda to process a given job
  * @tparam T the type of the job to be processed
  */
class ThreadManagerBuilder[T](lambda: T => Unit) {

  private val threadManager = new ThreadManager[T]()
  processingLambda(lambda)

  /** sets the processing lambda
    *
    * @param lambda the lambda to use for processing
    * @return the current ThreadManagerBuilder
    */
  private def processingLambda(lambda: T => Unit): ThreadManagerBuilder[T] = {
    threadManager.setLambda(lambda)
    this
  }

  def setLogLevel(level: LogLevel): ThreadManagerBuilder[T] = {
    threadManager.setLogLevel(level)
    this
  }

  /** if set the threads stop if the job queue runs empty
    *
    * @return the current builder
    */
  def stopOnEmpty(): ThreadManagerBuilder[T] = {
    threadManager.dieOnEmpty()
    this
  }
  @nowarn //this is an API call
  def addJob(job: T): ThreadManagerBuilder[T] = {
    threadManager.addJob(job)
    this
  }
  @nowarn //this is an API call
  def addJobs(jobs: Seq[T]): ThreadManagerBuilder[T] = {
    threadManager.addJobs(jobs)
    this
  }

  /** set the number of threads to be used in parallel (default: #cores)
    *
    * @param count the number of threads
    * @return the current ThreadManagerBuilder
    */
  def threadCount(count: Int): ThreadManagerBuilder[T] = {
    threadManager.setThreadCount(count)
    this
  }

  /** provide function to handle errors encountered during processing
    *
    * process the provided error. If some return, the error will be stored. If none return the error is discarded afterward
    *
    * @param onError the function to process the error
    * @return the current ThreadManagerBuilder
    */
  def handleErrors(onError: threadManager.OnErrorT): ThreadManagerBuilder[T] = {
    threadManager.setOnError(onError)
    this
  }

  /** create the configured ThreadManager
    *
    * @return the configured ThreadManager
    */
  def create(): ThreadManager[T] = threadManager.createPool()

  /** create the configured ThreadManager and start it as well
    *
    * @return the configured ThreadManager
    */
  def start(): ThreadManager[T] = create().start()

}
