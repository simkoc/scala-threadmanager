package de.halcony.threadmanager

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import wvlet.log.LogLevel.DEBUG
import wvlet.log.LogSupport

import java.util.concurrent.atomic.AtomicInteger

class ThreadManagerTest extends AnyWordSpec with Matchers with LogSupport {

  "processing using the thread manager" should {
      "process the queue" in {
        val manager = new ThreadManagerBuilder[Int](
          job => Thread.sleep(job)
        ).threadCount(5)
          .setLogLevel(DEBUG)
          .addJobs(List(1000,1000,1000,1000,1000)).start()
        assert(manager.waitFor(1500))
        manager.stop()
        assert(!manager.isAlive)
      }
    "handle errors default" in {
      val manager = new ThreadManagerBuilder[Int](
        job => throw new RuntimeException(s"bad juju $job")
      ).threadCount(5)
        .setLogLevel(DEBUG)
        .addJobs(List(1000,1000,1000,1000,1000)).start()
      assert(manager.waitFor(1500))
      manager.stop()
      assert(!manager.isAlive)
      assertResult(5)(manager.getErrors.length)
    }
    "handle errors custom" in {
      val counter : AtomicInteger = new AtomicInteger(0)
      val manager = new ThreadManagerBuilder[Int](
        job => throw new RuntimeException(s"bad juju $job")
      ).threadCount(5)
        .setLogLevel(DEBUG)
        .handleErrors{
          (job,thr) =>
            counter.incrementAndGet()
            None
        }
        .addJobs(List(1,1,1,1,1)).start()
      assert(manager.waitFor(1500))
      manager.stop()
      assert(!manager.isAlive)
      assertResult(0)(manager.getErrors.length)
      assertResult(5)(counter.get())
    }
    "terminate if takes too long" in {
      val counter : AtomicInteger = new AtomicInteger(0)
      val manager = new ThreadManagerBuilder[Int](
        job => Thread.sleep(job)
      ).threadCount(1)
        .setLogLevel(DEBUG)
        .stopOnEmpty()
        .handleErrors{
          (job,thr) =>
            counter.incrementAndGet()
            None
        }
        .addJobs(List(10000,10000,10000,10000,10000)).start()
      assert(!manager.waitFor(2000))
      assert(manager.isAlive)
      if(!manager.stop(1000)) {
        manager.destroyAndWait()
      }
      assert(!manager.isAlive)
      assertResult(4)(manager.remainingJobs())
    }
    "pause the threads and wait" in {
      val manager = new ThreadManagerBuilder[Int](
        job => Thread.sleep(job)
      ).threadCount(2)
        .setLogLevel(DEBUG)
        .stopOnEmpty()
        .addJobs(List(2000,2000,2000)).start()
      info("started + pausing")
      manager.pauseThreads()
      info("threads paused")
      if(manager.remainingJobs() == 2) {
        assertResult(2)(manager.remainingJobs())
      } else {
        assertResult(1)(manager.remainingJobs())
      }
      manager.unpauseThreads()
      manager.waitFor()
      assertResult(0)(manager.remainingJobs())
    }
  }

}
