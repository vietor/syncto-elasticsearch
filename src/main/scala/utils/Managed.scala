package mongodbsync.utils

object Managed {
  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }

  def cleanly[A, B](resource: A)(cleanup: A => Unit)(f: A => B): B = {
    try {
      f(resource)
    }
    finally {
      try {
        if(resource != null)
          cleanup(resource)
      } catch {
        case _: Throwable => {}
      }
    }
  }
}
