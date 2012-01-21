package org.wquery.utils

class Cache[A](refresh: () => A, threshold: Int) {
  private var stamp = 0
  private var obj = refresh()
  
  def get = {
    if (stamp >= threshold) {
      obj = refresh()
      stamp = 0
    }

    obj
  }

  def age { stamp += 1 }

  def invalidate { stamp = threshold }
}