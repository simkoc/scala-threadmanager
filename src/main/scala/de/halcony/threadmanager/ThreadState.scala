package de.halcony.threadmanager

import java.time.Instant

sealed trait ThreadState {
  val STATE_NAME: String
  private val since: Long = Instant.now().getEpochSecond

  override def toString: String = s"$STATE_NAME ($since)"
}

sealed case class Created() extends ThreadState {
  override val STATE_NAME: String = "Created"
}

sealed case class Running() extends ThreadState {
  override val STATE_NAME: String = "Running"
}

sealed case class Working[T](job: T) extends ThreadState {
  override val STATE_NAME: String = s"Working on $job"
}

sealed trait Stopped extends ThreadState

sealed case class Paused() extends Stopped {
  override val STATE_NAME: String = "Paused"
}

sealed trait DoneThreadState extends Stopped

sealed case class Interrupted() extends DoneThreadState {
  override val STATE_NAME: String = "Interrupted"
}

sealed case class Finished() extends DoneThreadState {
  override val STATE_NAME: String = "Finished"
}

sealed case class Crashed(thr: Throwable) extends DoneThreadState {
  override val STATE_NAME: String = s"Crashed with ${thr.getMessage}"
}
