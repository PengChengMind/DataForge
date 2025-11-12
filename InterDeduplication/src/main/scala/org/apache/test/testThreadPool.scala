package test

import java.util.concurrent.{Executor, Executors, Semaphore}


object testThreadPool {
  private val THREAD_COUNT = 3
  private val threadPool = Executors.newFixedThreadPool(THREAD_COUNT)

  def main(args: Array[String]): Unit = {
    for (i <- 0 until 30) {
      threadPool.execute(new Runnable() {
        override def run(): Unit = {
          try {
            System.out.println("save data")
            Thread.sleep(3000)
          } catch {
            case e: InterruptedException =>

          }
        }
      })
    }
    threadPool.shutdown
  }
}