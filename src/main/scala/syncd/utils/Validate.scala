package syncd.utils

import java.util.{List}

object Validate {

  def isNullOrEmpty[E](l: List[E]): Boolean = {
    l == null || l.size() < 1
  }

  def isNullOrBlank(s: String): Boolean = {
    s == null || s.isEmpty() || s.trim().isEmpty()
  }
}
