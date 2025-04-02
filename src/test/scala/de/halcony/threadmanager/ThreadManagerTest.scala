package de.halcony.threadmanager

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import wvlet.log.LogLevel.DEBUG

import java.util.concurrent.atomic.AtomicInteger

class ThreadManagerTest extends AnyWordSpec with Matchers{

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
        .addJobs(List(2500,10000,10000,10000,10000)).start()
      //Thread.sleep(1000) // wind up time due to parallelized starting
      assert(!manager.waitFor(2000))
      assert(manager.isAlive)
      if(!manager.stop(1000)) {
        manager.destroyAndWait()
      }
      assert(!manager.isAlive)
      assertResult(4)(manager.remainingJobs())
    }
  }

}
