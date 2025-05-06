package de.halcony.threadmanager

class ThreadManagerThread[T](id: Int, manager: ThreadManager[T])
    extends Thread {

  def getThreadIdentifier: Int = id

  private var state: ThreadState = Created()
  def getThreadState: ThreadState = synchronized {
    this.state
  }
  private def setThreadState(newState: ThreadState): Unit = synchronized {
    this.state = newState
  }

  def debug(msg: String): Unit = {
    manager.threadDebug(getThreadIdentifier, msg)
  }

  def info(msg: String): Unit = {
    manager.threadInfo(getThreadIdentifier, msg)
  }

  def warn(msg: String): Unit = {
    manager.threadWarn(getThreadIdentifier, msg)
  }

  def error(msg: String): Unit = {
    manager.threadError(getThreadIdentifier, msg)
  }

  override def run(): Unit = {
    try {
      this.setThreadState(Running())
      manager.threadInfo(getThreadIdentifier, "has started")
      // while the threads are supposed to run
      while (manager.getThreadsActive && !this.getThreadState
               .isInstanceOf[Finished]) {
        manager.synchronized {
          if (manager.threadsArePaused()) {
            this.setThreadState(Paused())
            manager.notifyAll()
            while (manager.threadsArePaused()) {
              manager.wait()
            }
          }
          // get the next job
          // if the job queue is empty
          if (manager.getJobQueue.isEmpty) {
            // and threads shall stop if there is no job left
            if (manager.getDieOnEmpty) {
              // set the thread state to None
              this.setThreadState(Finished())
            } else {
              // otherwise until you are notified
              manager.wait()
            }
            // but in any case there was no job
            None
          } else {
            Some(manager.getJobQueue.dequeue())
          }
        } match {
          // if there is a job
          case Some(job) =>
            try {
              // set your state to working
              this.setThreadState(Working(job))
              manager.threadDebug(getThreadIdentifier,
                                  s"started working on $job")
              // and apply the lambda
              manager.getJobLambda.apply(job)
            } catch {
              // if there was an interrupted exception
              case interrupted: InterruptedException =>
                // rethrow it
                throw interrupted
              // any other exception is to be caught
              case thr: Throwable =>
                debug(s"crashed while working on $job: ${thr.getMessage}")
                // and logged
                manager.encounteredError(Some(job), thr)
            } finally {
              debug(s"finished working on $job")
              // as the job (if there was any) is done, set the state to running
              this.setThreadState(Running())
              manager.synchronized {
                // and notify the manager
                manager.notifyAll()
              }
            }
          case None =>
        }
      }
    } catch {
      case _: InterruptedException => // if there was an interrupt, just continue
        this.setThreadState(Interrupted())
      case thr: Throwable =>
        error(s"encountered unmanaged error ${thr.getMessage}")
        manager.encounteredError(None, thr)
        this.setThreadState(Crashed(thr))
    } finally {
      info("is done")
      if (!this.getThreadState.isInstanceOf[DoneThreadState])
        this.setThreadState(Finished())
      manager.synchronized {
        manager.notifyAll()
      }
    }
  }

}
